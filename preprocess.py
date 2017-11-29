import os

import mysql.connector
import progressbar


def iter_row(cursor, size=100):
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        for row in rows:
            yield row

cnx = mysql.connector.connect(user='root', password='', host='127.0.0.1', database='facebook')
cursor = cnx.cursor()

if __name__ == '__main__':
    cursor.execute('SELECT count(*) FROM comment')
    count = cursor.fetchone()[0]
    bar = progressbar.ProgressBar(max_value=count)

    cursor.execute('SELECT message FROM comment ORDER BY RAND()')
    txt = ''
    for (message,) in bar(iter_row(cursor)):
        txt += message + '\n##\n'

with open("bla.txt", "wb") as f:
    f.write(txt.encode('utf8'))
