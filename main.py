import psycopg2
import json
import csv
import timeit

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


def doCopy(rex, cursor, conn):
    f = open('C:\\Users\\tzahr\\Documents\\authors.csv', 'r', encoding='utf-8')
    cursor.copy_from(f, 'authors', sep=';')
    conn.commit()
    rex += 1
    num = 1
    f.close()
    return rex, 1


def removeDupes(table, cursor, conn):
    cursor.execute(f"""DELETE FROM {table} a USING (
              SELECT MIN(ctid) as ctid, id
                FROM authors 
                GROUP BY id HAVING COUNT(*) > 1
              ) b
              WHERE a.id = b.id 
              AND a.ctid <> b.ctid""")
    conn.commit()


def importAuthors():
    with open('C:\\Users\\tzahr\\Downloads\\authors.jsonl', 'r') as json_file:
        rex = 1
        num = 1
        for line in json_file:
            # if rex == 11:
            #     break
            if num == 1:
                f = open('C:\\Users\\tzahr\\Documents\\authors.csv', 'w', newline='', encoding='utf-8')
                writer = csv.writer(f, delimiter=";")

            line = json.loads(line)
            row = [line['id'], line['name'], line['username'], line['description'],
                   line['public_metrics']['followers_count'],
                   line['public_metrics']['following_count'], line['public_metrics']['tweet_count'],
                   line['public_metrics']['listed_count']]
            for index, elem in enumerate(row):
                if index in range(1, 4):
                    row[index] = clearFields(elem)
            writer.writerow(row)
            row.clear()
            if num == 100000:
                f.close()
                rex, num = doCopy(rex, cursor, conn)
                continue
            num += 1

        f.close()
        rex, num = doCopy(rex, cursor, conn)

    removeDupes('authors', cursor, conn)

    cursor.execute("""ALTER TABLE authors
        ADD PRIMARY KEY (id),
        ALTER COLUMN ID SET NOT NULL,
        ALTER COLUMN name SET NOT NULL,
        ALTER COLUMN username SET NOT NULL,
        ALTER COLUMN description SET NOT NULL,
        ALTER COLUMN following_count SET NOT NULL,
        ALTER COLUMN followers_count SET NOT NULL,
        ALTER COLUMN tweet_count SET NOT NULL,
        ALTER COLUMN listed_count SET NOT NULL; """)
    conn.commit()

start = timeit.default_timer()
importAuthors()


stop = timeit.default_timer()
print('Time: ', stop - start)
