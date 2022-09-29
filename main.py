import psycopg2
import json
import csv
import timeit
import gzip
from datetime import datetime

conn = psycopg2.connect(
    database="postgres", user='postgres', password='postgres', host='localhost', port='5432'
)
cursor = conn.cursor()


def clearFields(elem):
    if '\x00' in elem:
        elem = elem.replace('\x00', '')
    if '\\' in elem:
        elem = elem.replace('\\', '')
    if ';' in elem:
        elem = elem.replace(';', ',')
    if '\n' in elem:
        elem = elem.replace('\n', '')
    if '\r' in elem:
        elem = elem.replace('\r', '')
    if elem == '':
        elem = 'NULL'

    return elem


def doCopyFrom(rex, cursor, conn, table):
    f = open(f'C:\\Users\\tzahr\\Documents\\{table}.csv', 'r', encoding='utf-8')
    cursor.copy_from(f, table, sep=';')
    conn.commit()
    rex += 1
    num = 1
    f.close()
    return rex, 1


def copyConv(rex, cursor, conn):
    f = open('C:\\Users\\tzahr\\Documents\\conv.csv', 'r', encoding='utf-8')
    cursor.copy_from(f, 'conversations', sep=';')
    conn.commit()
    rex += 1
    num = 1
    f.close()
    return rex, 1


def removeDupes(table, cursor, conn):
    cursor.execute(f"""DELETE FROM {table} a USING (
              SELECT MIN(ctid) as ctid, id
                FROM {table} 
                GROUP BY id HAVING COUNT(*) > 1
              ) b
              WHERE a.id = b.id 
              AND a.ctid <> b.ctid""")
    conn.commit()


def timer(seconds):
    min, sec = divmod(seconds, 60)
    return str(int(min))+':'+str(int(sec))

def importAuthors(start):
    print(datetime.now().isoformat() + ';', timer(timeit.default_timer() - start) + ';', timer(timeit.default_timer() - start))
    cursor.execute("""create table authors (
    id int8,
    name VARCHAR(255),
    username VARCHAR(255),
    description TEXT,
    followers_count int4,
    following_count int4,
    tweet_count int4,
    listed_count int4
);""")
    conn.commit()

    with open('C:\\Users\\tzahr\\Downloads\\authors.jsonl', 'r') as json_file:
        rex = 1
        num = 1
        for line in json_file:
            # if rex == 11:
            #     break
            if num == 1:
                blocktime = timeit.default_timer()
                f = open('C:\\Users\\tzahr\\Documents\\authors.csv', 'w', newline='', encoding='utf-8')
                writer = csv.writer(f, delimiter=";")

            line = json.loads(line)
            row = [line['id'], line['name'], line['username'], line['description'], line['public_metrics']['followers_count'],
                   line['public_metrics']['following_count'], line['public_metrics']['tweet_count'], line['public_metrics']['listed_count']]
            for index, elem in enumerate(row):
                if index in range(1, 4):
                    row[index] = clearFields(elem)
            writer.writerow(row)
            row.clear()
            if num == 100000:
                f.close()
                rex, num = doCopyFrom(rex, cursor, conn, 'authors')
                print(datetime.now().isoformat() + ';', timer(timeit.default_timer() - start) + ';', timer(timeit.default_timer() - blocktime))
                continue
            num += 1

        f.close()
        rex, num = doCopyFrom(rex, cursor, conn, 'authors')
        print(datetime.now().isoformat() + ';', timer(timeit.default_timer() - start) + ';', timer(timeit.default_timer() - blocktime))

    blocktime = timeit.default_timer()
    removeDupes('authors', cursor, conn)
    print('Cleaning up')
    cursor.execute("""ALTER TABLE authors
        ADD PRIMARY KEY (id),
        ALTER COLUMN name SET NOT NULL,
        ALTER COLUMN username SET NOT NULL,
        ALTER COLUMN description SET NOT NULL,
        ALTER COLUMN following_count SET NOT NULL,
        ALTER COLUMN followers_count SET NOT NULL,
        ALTER COLUMN tweet_count SET NOT NULL,
        ALTER COLUMN listed_count SET NOT NULL; """)
    conn.commit()
    print(datetime.now().isoformat() + ';', timer(timeit.default_timer() - start) + ';', timer(timeit.default_timer() - blocktime))


def importConv():
    with gzip.open('C:\\Users\\tzahr\\Downloads\\conversations.jsonl.gz', 'r') as json_file:
        rex = 1
        num = 1
        for line in json_file:
            if rex == 11:
                break
            if num == 1:
                conv = open('C:\\Users\\tzahr\\Documents\\conv.csv', 'w', newline='', encoding='utf-8')
                writer = csv.writer(conv, delimiter=";")

            line = json.loads(line)
            if line.get('conent'):
                print('a')
            row = [line['id'], line['author_id'], 'content placeholder', line['possibly_sensitive'], line['lang'], line['source'],
                   line['public_metrics']['retweet_count'], line['public_metrics']['reply_count'],
                   line['public_metrics']['like_count'], line['public_metrics']['quote_count'], line['created_at']]
            for index, elem in enumerate(row):
                if index in range(2, 4):
                    if index != 3:
                        row[index] = clearFields(elem)
            writer.writerow(row)
            row.clear()
            if num == 100000:
                conv.close()
                rex, num = copyConv(rex, cursor, conn)
                continue
            num += 1

        # conv.close()
        # rex, num = copyConv(rex, cursor, conn)

    removeDupes('conversations', cursor, conn)

    cursor.execute("""ALTER TABLE conversations
    ADD PRIMARY KEY (id),
    ALTER COLUMN like_count SET NOT NULL,
    ALTER COLUMN quote_count SET NOT NULL,
    ALTER COLUMN retweet_count SET NOT NULL,
    ALTER COLUMN reply_count SET NOT NULL;""")
    conn.commit()


start = timeit.default_timer()
importAuthors(start)
#importConv()


stop = timeit.default_timer()
print('Total time:', timer(stop - start))
