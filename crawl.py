from datetime import datetime, timedelta
import sys
import json
import re
import time
import mysql.connector
import facebook
import progressbar


def main():
    with open('config.json', 'r') as c:
        config = json.load(c)

    crawler = Crawler(config)
    crawler.crawl()

class Crawler:
    def __init__(self, config):
        self.pages = config["facebook"]["pages"]
        self.start_date = datetime.strptime(config["startDate"], "%Y-%m-%d")
        self.end_date = datetime.today() - timedelta(days=1)  # We only crawl until 24h before "now" to not miss out on new subcomments

        # Initialize database
        self.cnx = mysql.connector.connect(user=config["database"]["user"],
                                           password=config["database"]["password"],
                                           host=config["database"]["host"],
                                           database=config["database"]["db"])
        self.cursor = self.cnx.cursor()

        # Initialize Facebook Graph API
        token = facebook.GraphAPI().get_app_access_token(config["facebook"]["appId"], config["facebook"]["appSecret"], offline=False)
        #token = 'EAACEdEose0cBADJPw82v91QOlpksut3AI2fuRuZAffZAnuwC4duZAqw2JRVxfk8WjaRDmouojgclZCjZCD4JX14s4N7oohSdw0Y90DY4X4weRNBWSi3zzz8fDSbZCuZCFZBvPPUgLQbxAbAXAZAPyNDgZBtZBWB7JlZCZCymgo7qF8y1GBgavsgDWkZBsx3ZCzaZB93ZByNIKzOQTbaKtUgZDZD'
        self.graph = facebook.GraphAPI(access_token=token, version='2.10')


    def crawl(self):
        # Crawl pages
        for page_path in self.pages:
            self.cursor.execute('SELECT * FROM page WHERE path=%s', (page_path,))
            if len(self.cursor.fetchall()) == 0:
                page = self.graph.get_object(page_path)
                print('Inserting "www.facebook.com/{}": "{}"'.format(page_path, page['name']))
                self.cursor.execute('INSERT INTO page (fb_id, path, name) VALUES (%s,%s,%s)',
                                    (page['id'], page_path, page['name']))
                self.cnx.commit()

        # Crawl posts
        self.cursor.execute('SELECT name, id, fb_id FROM page')
        for (page_name, page_id, page_fb_id) in self.cursor.fetchall():
            # Compute start and end date
            self.cursor.execute('SELECT max(created_time) FROM post WHERE page=%s', (page_id,))
            latest_date = self.cursor.fetchone()[0]
            if latest_date is None:
                latest_date = self.start_date
            start_date = time.mktime(latest_date.timetuple())
            end_date = time.mktime(self.end_date.timetuple())

            # Download posts
            print('Crawling "{}" posts starting from {} ...'.format(page_name, latest_date.strftime('%B %d, %Y')), end='')
            posts = self.graph.get_all_connections(page_fb_id, 'posts', order='chronological', since=start_date, until=end_date, limit=100)
            counter = 0
            for post in posts:
                values = (page_id, post['id'], post['created_time'], post.get('story'), post.get('message'))
                success = self._insert_if_possible('INSERT INTO post (page, fb_id, created_time, story, message) VALUES (%s,%s,%s,%s,%s)', values)
                if success:
                    counter = counter + 1
            print(' {} new posts crawled'.format(counter))

        # Crawl comments
        bar = progressbar.ProgressBar()
        self.cursor.execute('SELECT id, page, fb_id, created_time FROM post WHERE do_not_crawl=0')
        fields = 'id,message,message_tags,from,created_time,comment_count,like_count'
        comment_counter = 0
        for (post_id, page_id, post_fb_id, post_created_time) in bar(self.cursor.fetchall()):
            self.cursor.execute('SELECT max(created_time) FROM comment WHERE post=%s', (post_id,))
            latest_date = self.cursor.fetchone()[0]
            if latest_date is None:
                comments = self.graph.get_all_connections(post_fb_id, 'comments', fields=fields, order='chronological', limit=100)
            else:
                start_date = time.mktime(latest_date.timetuple())
                comments = self.graph.get_all_connections(post_fb_id, 'comments', fields=fields, order='chronological', limit=100, since=start_date)
            for comment in comments:
                success = self._add_comment(comment, post_id, page_id)
                if success:
                    comment_counter = comment_counter + 1
                if success and comment['comment_count'] > 0:
                    self.cnx.commit()
                    comment_id = self.cursor.lastrowid
                    subcomments = self.graph.get_all_connections(comment['id'], 'comments', fields=fields, order='chronological', limit=500)
                    for subcomment in subcomments:
                        success = self._add_comment(subcomment, post_id, page_id, comment_id)
                        if success:
                            comment_counter = comment_counter + 1
                self.cnx.commit()
            # If all comments are crawled and post is older than 1 month, activate 'do_not_crawl' flag
            if post_created_time < (datetime.today() - timedelta(days=30)):
                self.cursor.execute('UPDATE post SET do_not_crawl=1 WHERE id=%s', (post_id,))

        print('{} new comments added'.format(comment_counter))

    def _insert_if_possible(self, query, values):
        try:
            self.cursor.execute(query, values)
            self.cnx.commit()
            return True
        except mysql.connector.errors.IntegrityError:
            self.cnx.rollback()
            return False

    def _add_comment(self, comment, post_id, page_id, parent_comment=None):
        user_id = self._get_or_create_user(comment['from'])
        message = self._clean_message(comment)
        if len(message) > 0:
            columns = '(user, post, page, fb_id, created_time, message, like_count, comment_count'
            values = (user_id, post_id, page_id, comment['id'], comment['created_time'],
                      message, comment['like_count'], comment['comment_count'])
            values_placeholder = '(%s,%s,%s,%s,%s,%s,%s,%s'
            if parent_comment is None:
                columns = columns + ')'
                values_placeholder = values_placeholder + ')'
            else:
                columns = columns + ',parent_comment)'
                values = values + (parent_comment,)
                values_placeholder = values_placeholder + ',%s)'
            return self._insert_if_possible('INSERT INTO comment {} VALUES {}'.format(columns, values_placeholder), values)
        else:
            return False

    def _get_or_create_user(self, user):
        self.cursor.execute('SELECT id FROM user WHERE fb_id=%s', (user['id'],))
        user_ids = self.cursor.fetchall()
        assert len(user_ids) <= 1, 'Too many users: ' + user_ids
        if len(user_ids) == 1:
            return user_ids[0][0]
        else:
            self.cursor.execute('INSERT INTO user (fb_id, name) VALUES (%s,%s)', (user['id'], user['name']))
            return self.cursor.lastrowid

    @staticmethod
    def _clean_message(self, comment):
        message = comment['message']
        # Remove comments with linked persons (they mostly contain only emojis)
        if 'message_tags' in comment:
            return ''
        # Remove comments with the hashtag #HassHilft (http://hasshilft.de/)
        if '#HassHilft' in message:
            return ''
        # Remove links
        message = re.sub(r'http\S+', '', message)
        return message.strip()


if __name__ == "__main__":
    main()