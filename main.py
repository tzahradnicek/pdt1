import psycopg2
import json
import csv
import timeit
import gzip
from datetime import datetime


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


def doCopyFrom(table, rex=None):
    f = open(f'C:\\Users\\tzahr\\Documents\\{table}.csv', 'r', encoding='utf-8')
    if table not in ['authors', 'conversations', 'context_domains', 'context_entities']:
        if table == 'hashtags':
            cursor.copy_from(f, table, sep=';', columns=['tag'])
        elif table == 'annotations':
            cursor.copy_from(f, table, sep=';', columns=('conversation_id', 'value', 'type', 'probability'))
        elif table == 'links':
            cursor.copy_from(f, table, sep=';', columns=('conversation_id', 'url', 'title', 'description'))
        elif table == 'conversation_references':
            cursor.copy_from(f, table, sep=';', columns=('conversation_id', 'parent_id', 'type'))
    else:
        cursor.copy_from(f, table, sep=';')
    f.close()
    if rex:
        rex += 1
        return rex, 1


def removeDupes(table, column='id'):
    cursor.execute(f"""DELETE FROM {table} a USING (
              SELECT MIN(ctid) as ctid, {column}
                FROM {table} 
                GROUP BY {column} HAVING COUNT(*) > 1
              ) b
              WHERE a.{column} = b.{column} 
              AND a.ctid <> b.ctid""")
    conn.commit()


def createConvTables():
    cursor.execute("""
    DROP TABLE IF EXISTS conversation_references;
    DROP TABLE IF EXISTS conversations;
    DROP TABLE IF EXISTS annotations;
    DROP TABLE IF EXISTS links;
    DROP TABLE IF EXISTS hashtags;
    DROP TABLE IF EXISTS context_domains;
    DROP TABLE IF EXISTS context_entities;
    create table context_entities (
        id int8,
        name VARCHAR(255),
        description text
    );

    create table context_domains (
        id int8,
        name VARCHAR(255),
        description text
    );

    create table conversation_references (
        id bigserial primary key,
        conversation_id int8,
        parent_id int8,
        type varchar(20)
    );

    create table annotations (
        id bigserial primary key,
        conversation_id int8,
        value text,
        type text,
        probability NUMERIC(4,3)
    );

    create table links (
        id bigserial primary key,
        conversation_id int8,
        url varchar(2048),
        title text,
        description text
    );


    create table hashtags (
        id bigserial primary key,
        tag text
    );

    create table conversations (
        id int8,
        author_id int8,
        content TEXT,
        possibly_sensitive bool,
        language varchar(3),
        source text,
        retweet_count int4,
        reply_count int4,
        like_count int4,
        quote_count int4,
        created_at TIMESTAMP
    );
    """)
    conn.commit()


def cleanConvTables():
    blocktime = timeit.default_timer()
    print('Cleaning up conversations + hashtags')
    removeDupes('conversations')
    removeDupes('hashtags', column='tag')

    cursor.execute("""ALTER TABLE conversations
            ADD PRIMARY KEY (id),
            ALTER COLUMN author_id SET NOT NULL,
            ALTER COLUMN content SET NOT NULL,
            ALTER COLUMN possibly_sensitive SET NOT NULL,
            ALTER COLUMN language SET NOT NULL,
            ALTER COLUMN source SET NOT NULL,
            ALTER COLUMN created_at SET NOT NULL;""")
    conn.commit()
    print(datetime.now().isoformat() + ';', timer(timeit.default_timer() - start) + ';', timer(timeit.default_timer() - blocktime))

    blocktime = timeit.default_timer()
    print('Linking conversations with authors')
    cursor.execute("""
        insert into authors (id)
            select distinct author_id from conversations conv where conv.author_id not in (
                select id from authors auth where auth.id = conv.author_id; )

        alter table conversations
        add foreign key (author_id) REFERENCES authors (id);""")
    conn.commit()
    print(datetime.now().isoformat() + ';', timer(timeit.default_timer() - start) + ';', timer(timeit.default_timer() - blocktime))

    blocktime = timeit.default_timer()
    print('Linking conversation_references with conversations')
    cursor.execute("""
        delete from conversation_references convref where convref.parent_id in (
            select distinct parent_id from conversation_references cref where parent_id not in (
                select id from conversations conv where conv.id = cref.parent_id));

        alter table conversation_references
        add foreign key (parent_id) REFERENCES conversations (id);""")
    conn.commit()
    print(datetime.now().isoformat() + ';', timer(timeit.default_timer() - start) + ';', timer(timeit.default_timer() - blocktime))


def timer(seconds):
    min, sec = divmod(seconds, 60)
    return str(int(min))+':'+str(int(sec))


def writeConvCSV(annotations, annotWriter, hashtags, hashWriter, links, linkWriter, refs, refWriter, cont_dom, cont_domWriter, cont_ent, cont_entWriter):
    if annotations:
        for annot in annotations:
            annotWriter.writerow(annot)
    if hashtags:
        for hasht in hashtags:
            hashWriter.writerow(hasht)
    if links:
        for link in links:
            linkWriter.writerow(link)
    if refs:
        for ref in refs:
            refWriter.writerow(ref)
    if cont_dom:
        for cont in cont_dom:
            cont_domWriter.writerow(cont)
    if cont_ent:
        for cont in cont_ent:
            cont_entWriter.writerow(cont)


def importAuthors():
    print('Starting Authors import')
    print(datetime.now().isoformat() + ';', timer(timeit.default_timer() - start) + ';', timer(timeit.default_timer() - start))
    cursor.execute("""
    drop table if exists authors;
    create table authors (
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
                rex, num = doCopyFrom('authors', rex=rex)
                conn.commit()
                print(datetime.now().isoformat() + ';', timer(timeit.default_timer() - start) + ';', timer(timeit.default_timer() - blocktime))
                continue
            num += 1

        f.close()
        doCopyFrom('authors', rex=rex)
        conn.commit()
        print(datetime.now().isoformat() + ';', timer(timeit.default_timer() - start) + ';', timer(timeit.default_timer() - blocktime))

    blocktime = timeit.default_timer()
    removeDupes('authors')
    print('Cleaning up authors')
    cursor.execute("""ALTER TABLE authors
        ADD PRIMARY KEY (id);""")
    conn.commit()
    print(datetime.now().isoformat() + ';', timer(timeit.default_timer() - start) + ';', timer(timeit.default_timer() - blocktime))


def importConv():
    print('Starting Conversations + linked table import')
    print(datetime.now().isoformat() + ';', timer(timeit.default_timer() - start) + ';', timer(timeit.default_timer() - start))
    with gzip.open('C:\\Users\\tzahr\\Downloads\\conversations.jsonl.gz', 'r') as json_file:
        rex = 1
        num = 1
        for line in json_file:
            annotations = None
            hashtags = None
            links = None
            references = None
            context_dom = None
            context_ent = None
            if rex == 51:
                break
            if num == 1:
                blocktime = timeit.default_timer()
                conv = open('C:\\Users\\tzahr\\Documents\\conversations.csv', 'w', newline='', encoding='utf-8')
                convWriter = csv.writer(conv, delimiter=";")
                annot = open('C:\\Users\\tzahr\\Documents\\annotations.csv', 'w', newline='', encoding='utf-8')
                annotWriter = csv.writer(annot, delimiter=";")
                hash = open('C:\\Users\\tzahr\\Documents\\hashtags.csv', 'w', newline='', encoding='utf-8')
                hashWriter = csv.writer(hash, delimiter=";")
                link = open('C:\\Users\\tzahr\\Documents\\links.csv', 'w', newline='', encoding='utf-8')
                linkWriter = csv.writer(link, delimiter=";")
                ref = open('C:\\Users\\tzahr\\Documents\\conversation_references.csv', 'w', newline='', encoding='utf-8')
                refWriter = csv.writer(ref, delimiter=";")
                cont_dom = open('C:\\Users\\tzahr\\Documents\\context_domains.csv', 'w', newline='', encoding='utf-8')
                cont_domWriter = csv.writer(cont_dom, delimiter=";")
                cont_ent = open('C:\\Users\\tzahr\\Documents\\context_entities.csv', 'w', newline='', encoding='utf-8')
                cont_entWriter = csv.writer(cont_ent, delimiter=";")

            line = json.loads(line)
            row = [line['id'], line['author_id'], line['text'], line['possibly_sensitive'], line['lang'], line['source'],
                   line['public_metrics']['retweet_count'], line['public_metrics']['reply_count'],
                   line['public_metrics']['like_count'], line['public_metrics']['quote_count'], line['created_at']]

            if line.get('entities'):
                if line['entities'].get('annotations'):
                    annotations_list = line['entities']['annotations']
                    annotations = []
                    for i in annotations_list:
                        annotations.append([line['id'], clearFields(i['normalized_text']), clearFields(i['type']), i['probability']])
                if line['entities'].get('hashtags'):
                    hashtags_list = line['entities']['hashtags']
                    hashtags = []
                    for i in hashtags_list:
                        hashtags.append([clearFields(i['tag'])])
                if line['entities'].get('urls'):
                    links_list = line['entities']['urls']
                    links = []
                    for i in links_list:
                        links.append([line['id'], clearFields(i['expanded_url']), clearFields(i['title']) if i.get('title') else None,
                                      clearFields(i['description']) if i.get('description') else None])

            if line.get('referenced_tweets'):
                refs_list = line['referenced_tweets']
                references = []
                for i in refs_list:
                    references.append([line['id'], i['id'], clearFields(i['type'])])

            if line.get('context_annotations'):
                context_list = line['context_annotations']
                context_dom = []
                context_ent = []
                for i in context_list:
                    context_dom.append([i['domain']['id'], clearFields(i['domain']['name']), clearFields(i['domain']['description']) if i['domain'].get('description') else None])
                    context_ent.append([i['domain']['id'], clearFields(i['entity']['name']), clearFields(i['entity']['description']) if i['entity'].get('description') else None])

            for index, elem in enumerate(row):
                if index in range(2, 4):
                    if index != 3:
                        row[index] = clearFields(elem)
            writeConvCSV(annotations, annotWriter, hashtags, hashWriter, links, linkWriter, references, refWriter, context_dom, cont_domWriter, context_ent, cont_entWriter)
            convWriter.writerow(row)
            row.clear()
            if num == 100000:
                conv.close()
                annot.close()
                hash.close()
                link.close()
                ref.close()
                cont_dom.close()
                cont_ent.close()
                rex, num = doCopyFrom('conversations', rex=rex)
                doCopyFrom('links')
                doCopyFrom('hashtags')
                doCopyFrom('annotations')
                doCopyFrom('conversation_references')
                doCopyFrom('context_domains')
                doCopyFrom('context_entities')
                conn.commit()
                print(datetime.now().isoformat() + ';', timer(timeit.default_timer() - start) + ';', timer(timeit.default_timer() - blocktime))
                continue
            num += 1

        conv.close()
        annot.close()
        hash.close()
        link.close()
        ref.close()
        cont_dom.close()
        cont_ent.close()
        rex, num = doCopyFrom('conversations', rex=rex)
        doCopyFrom('links')
        doCopyFrom('hashtags')
        doCopyFrom('annotations')
        doCopyFrom('conversation_references')
        doCopyFrom('context_domains')
        doCopyFrom('context_entities')
        conn.commit()
        print(datetime.now().isoformat() + ';', timer(timeit.default_timer() - start) + ';', timer(timeit.default_timer() - blocktime))

    cleanConvTables()


conn = psycopg2.connect(database="postgres", user='postgres', password='postgres', host='localhost', port='5432')
cursor = conn.cursor()
start = timeit.default_timer()

#importAuthors()
createConvTables()
importConv()


stop = timeit.default_timer()
print('Total time:', timer(stop - start))
