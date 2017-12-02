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

    generate_text(args, config)


def iter_row(cursor, size=1000):
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        for row in rows:
            yield row


def remove_rare_characters(text):
    """Removes all characters that appear in less than 0.002% of the cases"""
    vocab_counter = collections.Counter()  # 750
    vocab_counter.update(text)

    threshold = len(text) * 0.00002
    chars_to_remove = []
    for char, count in reversed(vocab_counter.most_common()):
        if count < threshold:
            chars_to_remove.append(char)
        else:
            break

    return re.sub('[' + re.escape(''.join(chars_to_remove)) + ']', '', text)


def generate_text(args, config):
    cnx = mysql.connector.connect(user=config["database"]["user"],
                                  password=config["database"]["password"],
                                  host=config["database"]["host"],
                                  database=config["database"]["db"])
    cursor = cnx.cursor()

    cursor.execute('SELECT count(*) FROM comment')
    count = cursor.fetchone()[0]
    bar = progressbar.ProgressBar(max_value=count)

    cursor.execute('''
        SELECT p.message,
            p.created_time as primary_sort,
            Null as secondary_sort
        FROM comment p
        WHERE p.parent_comment IS NULL
        UNION
        SELECT c.message,
            p.created_time as primary_sort,
            c.created_time as secondary_sort
        FROM comment c
        JOIN comment p on p.id = c.parent_comment
        ORDER BY primary_sort ASC,
            secondary_sort ASC
    ''')
    txt = ''
    for (message, _, _) in bar(iter_row(cursor)):
        lines = message.replace('\r', '\n').split('\n')
        for line in lines:
            if 4 < len(line) < 1000 and '@ ' not in line:
                txt += '> {}\n'.format(line.strip())

    txt = remove_rare_characters(txt)

    with open(args.out, "wb") as f:
        f.write(txt.encode("cp1252", errors="ignore"))


if __name__ == '__main__':
    main()