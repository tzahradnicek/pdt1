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
        elem = None

    return elem


def doCopyFrom(cursor, conn, table, rex=None):
    f = open(f'C:\\Users\\tzahr\\Documents\\{table}.csv', 'r', encoding='utf-8')
    if table not in ['authors', 'conversations']:
        if table == 'hashtags':
            cursor.copy_from(f, table, sep=';', columns=['tag'])
        elif table == 'annotations':
            cursor.copy_from(f, table, sep=';', columns=('conversation_id', 'value', 'type', 'probability'))
        elif table == 'links':
            cursor.copy_from(f, table, sep=';', columns=('conversation_id', 'url', 'title', 'description'))
    else:
        cursor.copy_from(f, table, sep=';')
    conn.commit()
    f.close()
    if rex:
        rex += 1
        return rex, 1


def removeDupes(table, cursor, conn, column='id'):
    cursor.execute(f"""DELETE FROM {table} a USING (
              SELECT MIN(ctid) as ctid, {column}
                FROM {table} 
                GROUP BY {column} HAVING COUNT(*) > 1
              ) b
              WHERE a.{column} = b.{column} 
              AND a.ctid <> b.ctid""")
    conn.commit()


def timer(seconds):
    min, sec = divmod(seconds, 60)
    return str(int(min))+':'+str(int(sec))


def writeConvCSV(annotations, annotWriter, hashtags, hashWriter, links, linkWriter):
    if annotations:
        for annot in annotations:
            annotWriter.writerow(annot)
    if hashtags:
        for hasht in hashtags:
            hashWriter.writerow(hasht)
    if links:
        for link in links:
            linkWriter.writerow(link)


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
                rex, num = doCopyFrom(cursor, conn, 'authors', rex=rex)
                print(datetime.now().isoformat() + ';', timer(timeit.default_timer() - start) + ';', timer(timeit.default_timer() - blocktime))
                continue
            num += 1

        f.close()
        rex, num = doCopyFrom(cursor, conn, 'authors', rex=rex)
        print(datetime.now().isoformat() + ';', timer(timeit.default_timer() - start) + ';', timer(timeit.default_timer() - blocktime))

    blocktime = timeit.default_timer()
    removeDupes('authors', cursor, conn)
    print('Cleaning up authors')
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
            annotations = None
            hashtags = None
            links = None
            if rex == 11:
                break
            if num == 1:
                conv = open('C:\\Users\\tzahr\\Documents\\conversations.csv', 'w', newline='', encoding='utf-8')
                convWriter = csv.writer(conv, delimiter=";")
                annot = open('C:\\Users\\tzahr\\Documents\\annotations.csv', 'w', newline='', encoding='utf-8')
                annotWriter = csv.writer(annot, delimiter=";")
                hash = open('C:\\Users\\tzahr\\Documents\\hashtags.csv', 'w', newline='', encoding='utf-8')
                hashWriter = csv.writer(hash, delimiter=";")
                link = open('C:\\Users\\tzahr\\Documents\\links.csv', 'w', newline='', encoding='utf-8')
                linkWriter = csv.writer(link, delimiter=";")

            #entities -> hashtag url/link, annotations
            #referenced_tweets -> references
            #context annotations -> domain/entity
            line = json.loads(line)
            row = [line['id'], line['author_id'], line['text'], line['possibly_sensitive'], line['lang'], line['source'],
                   line['public_metrics']['retweet_count'], line['public_metrics']['reply_count'],
                   line['public_metrics']['like_count'], line['public_metrics']['quote_count'], line['created_at']]

            if line.get('entities'):
                if line['entities'].get('annotations'):
                    annotations_list = line['entities']['annotations']
                    annotations = []
                    for i in range(0, len(annotations_list)):
                        annotations.append([line['id'], clearFields(line['entities']['annotations'][i]['normalized_text']),
                                            clearFields(line['entities']['annotations'][i]['type']), line['entities']['annotations'][i]['probability']])
                if line['entities'].get('hashtags'):
                    hashtags_list = line['entities']['hashtags']
                    hashtags = []
                    for i in range(0, len(hashtags_list)):
                        hashtags.append([clearFields(line['entities']['hashtags'][i]['tag'])])
                if line['entities'].get('urls'):
                    links_list = line['entities']['urls']
                    links = []
                    for i in range(0, len(links_list)):
                        links.append([line['id'], clearFields(line['entities']['urls'][i]['expanded_url']),
                                      clearFields(line['entities']['urls'][i]['title']) if line['entities']['urls'][i].get('title') else None,
                                      clearFields(line['entities']['urls'][i]['description']) if line['entities']['urls'][i].get('description') else None])

            for index, elem in enumerate(row):
                if index in range(2, 4):
                    if index != 3:
                        row[index] = clearFields(elem)
            writeConvCSV(annotations, annotWriter, hashtags, hashWriter, links, linkWriter)
            convWriter.writerow(row)
            row.clear()
            if num == 100000:
                conv.close()
                annot.close()
                hash.close()
                link.close()
                rex, num = doCopyFrom(cursor, conn, 'conversations', rex=rex)
                doCopyFrom(cursor, conn, 'links')
                doCopyFrom(cursor, conn, 'hashtags')
                doCopyFrom(cursor, conn, 'annotations')
                continue
            num += 1

        # conv.close()
        # rex, num = copyConv(rex, cursor, conn)

    removeDupes('conversations', cursor, conn)
    removeDupes('hashtags', cursor, conn, column='tag')

    # cursor.execute("""ALTER TABLE conversations
    # ADD PRIMARY KEY (id),
    # ALTER COLUMN like_count SET NOT NULL,
    # ALTER COLUMN quote_count SET NOT NULL,
    # ALTER COLUMN retweet_count SET NOT NULL,
    # ALTER COLUMN reply_count SET NOT NULL;""")
    # conn.commit()


start = timeit.default_timer()
#importAuthors(start)
importConv()


stop = timeit.default_timer()
print('Total time:', timer(stop - start))
