from datetime import datetime, timedelta
import sys
import time
import mysql.connector
import facebook
import progressbar

# You have to register a Facebook App (https://developers.facebook.com/) to retrieve an App ID and Secret.
# No further configurations required.
FACEBOOK_APP_ID = '*****'
FACEBOOK_APP_SECRET = '*****'

# Any list of facebook page IDs that the account connected to your App has access to.
FACEBOOK_SITES = [
    'npd.de', 
    'npdsaar', 
    'npdhamburg', 
    'NpdBremen', 
    'npdthueringen',
    'npd.niedersachsen',
    'NPDSchleswigHolstein',
    'npdmup',
    'npd.sachsen',
    'npdbayern',
    'npd.brandenburg',
    'jugend.waehlt.npd',
    'ring.nationaler.frauen',
    'FamilieVolkundHeimat',
    'junge.nationalisten', 
    'DeutschlandGegenKindesmissbrauch',
    'patriotAUF',
    'wfdwirfeurdeutschland',
    'MutZurWahrheit2017'
]

# Initialize/reset db:  mysql -u root facebook < schema.sql
# Save schema changes:  mysqldump --no-data -u root facebook > schema.sql
MYSQL_HOST = '127.0.0.1'
MYSQL_USER = 'root'
MYSQL_PASSWORD = ''
MYSQL_DB = 'facebook'

# Example datetime window: September 2017
START_DATE = datetime(2017, 9,  1)
END_DATE   = datetime(2017, 9, 30, 23, 59, 59, 999)

def main():
    crawler = Crawler(FACEBOOK_APP_ID, FACEBOOK_APP_SECRET)
    while True:
        try:
            crawler.crawl(FACEBOOK_SITES)
            break
        except Exception as e:
            print(e)
            time.sleep(5000)
            main()

class Crawler:
    def __init__(self, facebook_app_id, facebook_app_secret):
        # Initializing database
        self.cnx = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, database=MYSQL_DB)
        self.cursor = self.cnx.cursor()
        # Initializing Facebook Graph API
        token = facebook.GraphAPI().get_app_access_token(FACEBOOK_APP_ID, FACEBOOK_APP_SECRET, offline=False)
        self.graph = facebook.GraphAPI(access_token=token, version='2.7')
        # Clean up any messy state from previous runs
        self._clean_up()

    def crawl(self, pages):
        self.crawl_pages(pages)
        self.crawl_posts(START_DATE, END_DATE)
        self.crawl_comments()
        self.crawl_subcomments()
        #self.crawl_likes()

    def crawl_pages(self, paths):
        print('Crawling pages...')
        for path in paths:
            self.cursor.execute('SELECT * FROM page WHERE path=%s', (path,))
            if len(self.cursor.fetchall()) == 0:
                page = self.graph.get_object(path)
                print('Inserting "www.facebook.com/{}": "{}"'.format(path, page['name']))
                self.cursor.execute('INSERT INTO page (fb_id, path, name) VALUES (%s,%s,%s)', (page['id'], path, page['name']))
                self.cnx.commit()

    def crawl_posts(self, start_date, end_date):
        print('\nCrawling posts...')
        self.cursor.execute('SELECT name, id, fb_id FROM page WHERE crawled=0')
        for (page_name, page_id, page_fb_id) in self.cursor.fetchall():
            print('Downloading posts of "{0}"...'.format(page_name))
            self.cursor.execute('UPDATE page SET in_progress=1 WHERE id=%s', (page_id,))
            self.cnx.commit()
            posts = self.graph.get_all_connections(page_fb_id, 'posts', since=time.mktime(start_date.timetuple()), until=time.mktime(end_date.timetuple()), limit=100)
            for post_number, post in enumerate(posts):
                if post_number > 0 and post_number % 100 == 100:
                    print('Downloaded {0} posts'.format(post_number))
                values = (page_id, post['id'], post['created_time'], post.get('story'), post.get('message'))
                try:
                    self.cursor.execute('INSERT INTO post (page, fb_id, created_time, story, message)'
                                'VALUES (%s,%s,%s,%s,%s)', values)
                    self.cnx.commit()
                except mysql.connector.errors.IntegrityError as error:
                    print(error)
                    self.cnx.rollback()
            self.cursor.execute('UPDATE page SET in_progress=0,crawled=1 WHERE id=%s', (page_id,))
            self.cnx.commit()

    def crawl_comments(self):
        print('Crawling comments...')
        bar = progressbar.ProgressBar()
        self.cursor.execute('SELECT id, page, fb_id FROM post WHERE crawled=0')
        for (post_id, page_id, post_fb_id) in bar(self.cursor.fetchall()):
            fields = 'id,message,from,created_time,like_count,comment_count'
            comments = self.graph.get_all_connections(post_fb_id, 'comments', fields=fields, limit=100)
            self.cursor.execute('UPDATE post SET in_progress=1 WHERE id=%s', (post_id,))
            self.cnx.commit()
            for comment in comments:
                user_id = self._get_or_create_user(comment['from'])
                values = (user_id, post_id, page_id, comment['id'], comment['created_time'],
                        comment['message'], comment['like_count'], comment['comment_count'])
                self.cursor.execute('INSERT INTO comment '
                            '(user, post, page, fb_id, created_time, message, like_count, comment_count) '
                            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s)', values)
                self.cnx.commit()
            self.cursor.execute('UPDATE post SET in_progress=0,crawled=1 WHERE id=%s', (post_id,))
            self.cnx.commit()

    def crawl_subcomments(self):
        print('Crawling subcomments...')
        self.cursor.execute('SELECT id, page, fb_id FROM comment WHERE crawled=0 AND comment_count > 0')
        rows = self.cursor.fetchall()
        bar = progressbar.ProgressBar(max_value=len(rows))
        for (parent_comment_id, page_id, parent_comment_fb_id) in bar(rows):
            self.cursor.execute('UPDATE comment SET in_progress=1 WHERE id=%s', (parent_comment_id,))
            self.cnx.commit()
            fields = ('id,message,from,created_time,like_count,comment_count')
            self._add_subcomments(parent_comment_id, parent_comment_fb_id, page_id, fields)
            self.cursor.execute('UPDATE comment SET in_progress=0,crawled=1 WHERE id=%s', (parent_comment_id,))
            self.cnx.commit()

    def crawl_likes(self):
        self.cursor.execute('SELECT id, fb_id, user FROM comment WHERE likes_crawled=0 AND like_count > 0')
        rows =  self.cursor.fetchall()
        bar = progressbar.ProgressBar(max_value=len(rows))
        for (comment_id, comment_fb_id, commenter_id) in bar(rows):
            self._add_likes(comment_fb_id, commenter_id)
            self.cursor.execute('UPDATE comment SET likes_crawled=1 WHERE id=%s', (comment_id,))
            self.cnx.commit()

    def _add_subcomments(self, parent_comment_id, parent_comment_fb_id, page_id, fields):
        try:
            subcomments = self.graph.get_all_connections('268232929583_' + parent_comment_fb_id, 'comments', fields=fields, limit=500)
            for subcomment in subcomments:
                user_id = self._get_or_create_user(subcomment['from'])
                values = (user_id, parent_comment_id, page_id, subcomment['id'], subcomment['created_time'],
                        subcomment['message'], subcomment['like_count'], subcomment['comment_count'])
                self.cursor.execute('INSERT INTO comment '
                            '(user, parent_comment, page, fb_id, created_time, message, like_count, comment_count) '
                            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s)', values)
        except facebook.GraphAPIError as error:
            if 'Unsupported get request. Object with ID' in error.message:
                print('Skipping ' + parent_comment_fb_id)
            else:
                raise error

    def _add_likes(self, comment_fb_id, commenter_id):
        try:
            likers = self.graph.get_all_connections(comment_fb_id, 'likes', limit=500)
            for liker in likers:
                liker_id = self._get_or_create_user(liker)
                self.cursor.execute('SELECT count FROM fb_like WHERE liker=%s AND commenter=%s', (liker_id, commenter_id))
                like = self.cursor.fetchone()
                if like:
                    self.cursor.execute('UPDATE fb_like SET count=%s WHERE liker=%s AND commenter=%s', (like[0]+1, liker_id, commenter_id))
                else:
                    self.cursor.execute('INSERT INTO fb_like VALUES (%s,%s,%s)', (liker_id, commenter_id, 1))
        except facebook.GraphAPIError as error:
            if 'Unsupported get request. Object with ID' in error.message:
                pass
                # print('Skipping ' + comment_fb_id)
            elif 'Please retry your request later.' in error.message:
                'RETRY'
                self._add_likes(comment_fb_id, commenter_id)
            else:
                raise error.message

    def _get_or_create_user(self, user):
        self.cursor.execute('SELECT id FROM user WHERE fb_id=%s', (user['id'],))
        user_ids = self.cursor.fetchall()
        assert len(user_ids) <= 1, 'Too many users: ' + user_ids
        if len(user_ids) == 1:
            return user_ids[0][0]
        else:
            self.cursor.execute('INSERT INTO user (fb_id, name) VALUES (%s,%s)', (user['id'], user['name']))
            return self.cursor.lastrowid

    def _clean_up(self):
        for (el, col, subel) in [('page', 'page', 'post'), ('post', 'post', 'comment'), ('comment', 'parent_comment', 'comment')]:
            self.cursor.execute('SELECT id FROM page WHERE in_progress=1')
            for (dirty_id,) in self.cursor.fetchall():
                print('Cleaning {}: {}'.format(el, dirty_id))
                self.cursor.execute('DELETE FROM {0} WHERE {1}=%s'.format(subel, col), (dirty_id,))
                self.cursor.execute('UPDATE {0} SET in_progress=0 WHERE id=%s'.format(el), (dirty_id,))
        self.cnx.commit()

if __name__ == "__main__":
    main()