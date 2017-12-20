import mysql.connector
import progressbar
import argparse
import yaml
import re
import collections


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', type=str, default='data/racist/racist.txt',
                        help='text file where the data is written to')
    args = parser.parse_args()

    with open('config.yml', 'r') as c:
        config = yaml.load(c)

    generate_dataset(args, config)


def iter_row(cursor, size=1000):
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        for row in rows:
            yield row


def generate_dataset(args, config):
    cnx = mysql.connector.connect(user=config["database"]["user"],
                                  password=config["database"]["password"],
                                  host=config["database"]["host"],
                                  database=config["database"]["db"])
    cursor = cnx.cursor()

    cursor.execute('SELECT count(*) FROM comment')
    count = cursor.fetchone()[0]
    bar = progressbar.ProgressBar(max_value=count)

    # This query groups comments by posts and places subcomments after their parent comments to
    # have as much context between the comments as possible. Everything is sorted ASC by date.
    cursor.execute('''
        # Parent comments
        SELECT p.message,
            user.name,
            post.created_time as post_created_time,
            p.created_time as comment_created_time,
            Null as subcomment_created_time
        FROM comment p
        JOIN user ON user.id = p.user
        JOIN post ON post.id = p.post
        WHERE p.parent_comment IS NULL
        UNION
        # Child comments
        SELECT c.message,
            user.name,
            post.created_time as post_created_time,
            p.created_time as comment_created_time,
            c.created_time as subcomment_created_time
        FROM comment c
        JOIN user ON user.id = c.user
        JOIN comment p on p.id = c.parent_comment
        JOIN post ON post.id = p.post
        ORDER BY post_created_time ASC,
            comment_created_time ASC,
            subcomment_created_time ASC
    ''')

    ds = Dataset()
    # As people tend to reference other people in subcomments, we collect the names of
    # all subcomment authors to remove them from the result in the end.
    authors = []
    comments = []
    for (message, author, post_date, comment_date, subcomment_date) in bar(iter_row(cursor)):
        if subcomment_date is None:  # is parent
            ds.push(comments, authors)
            authors = [author]
            comments = [message]
        else:  # is child
            authors.append(author)
            comments.append(message)
    ds.write(args.out)


class Dataset:
    def __init__(self):
        self.text = ''
        self.vocab_counter = collections.Counter()

    def write(self, outfile):
        self.remove_rare_characters()
        ending = outfile.split('.')[-1]
        if ending == 'txt':
            with open(outfile, "wb") as f:
                f.write(self.text.encode("cp1252", errors="ignore"))
        # TODO add bzip
        else:
            raise Exception  # TODO

    def push(self, comments, authors):
        lines = []
        for comment in comments:
            lines.extend(comment.replace('\r', '\n').split('\n'))
        txt = ''
        for line in lines:
            line = self.clean_tags(line)
            if 4 < len(line) < 500:
                txt += '> {}\n'.format(line)
        escaped_authors = [re.escape(author) for author in authors]
        txt = re.sub('({})'.format('|'.join(escaped_authors)), '', txt)
        self.text += txt
        self.vocab_counter.update(txt)

    def clean_tags(self, text):
        """Removing @ tags that the crawler was not able to filter out because they were not returned in message_tags"""
        if len(text) == 0 or ('@' in text and len(text.split(' ')) <= 3):
            return ''
        if text[0] == '@':
            text = re.sub('@ ?.*?((:|,|\.| {2})| .*?[:,. ])', '', text)
        else:
            text = re.sub('@', '', text)
        return text.strip()

    def remove_rare_characters(self):
        """Removes all characters that appear in less than 0.002% of the cases"""
        threshold = len(self.text) * 0.00002
        chars_to_remove = []
        for char, count in reversed(self.vocab_counter.most_common()):
            if count < threshold:
                chars_to_remove.append(char)
            else:
                break
        self.text = re.sub('[' + re.escape(''.join(chars_to_remove)) + ']', '', self.text)

    def merge_lines(self, lines):
        """Cleans and selects qualifying lines and merges them to a string"""
        txt = ''
        for line in lines:
            line = self.clean_tags(line)
            if 4 < len(line) < 500:
                txt += '> {}\n'.format(line)
        return txt


if __name__ == '__main__':
    main()