import mysql.connector
import progressbar
import argparse
import yaml


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', type=str, default='data/racist/racist.txt',
                        help='text file where the data is written to')
    args = parser.parse_args()

    with open('config.yml', 'r') as c:
        config = yaml.load(c)

    preprocess(args, config)

def iter_row(cursor, size=100):
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        for row in rows:
            yield row

def preprocess(args, config):
    cnx = mysql.connector.connect(user=config["database"]["user"],
                                  password=config["database"]["password"],
                                  host=config["database"]["host"],
                                  database=config["database"]["db"])
    cursor = cnx.cursor()

    cursor.execute('SELECT count(*) FROM comment')
    count = cursor.fetchone()[0]
    bar = progressbar.ProgressBar(max_value=count)

    cursor.execute('SELECT message FROM comment ORDER BY RAND()')
    txt = ''
    for (message,) in bar(iter_row(cursor)):
        parts = message.replace('\r', '\n').split('\n')
        for part in parts:
            if 4 < len(part) < 1000:
                txt += '> {}\n'.format(part.strip())

    with open(args.out, "wb") as f:
        f.write(txt.encode('utf8'))


if __name__ == '__main__':
	main()