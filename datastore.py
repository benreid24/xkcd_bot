import sqlite3
import os
import urllib.request
import json
import logging

logger = logging.getLogger(__name__)


def connect_datastore(empty=False):
    if empty:
        os.remove('database.db')
    conn = sqlite3.connect('database.db')
    fetch_comic_info(None)

    query = """CREATE TABLE IF NOT EXISTS links
                (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   CommentId varchar(64) NOT NULL UNIQUE,
                   Text varchar(1024) NOT NULL,
                   Link varchar(256) NOT NULL,
                   Comic int NOT NULL,
                   ParentId varchar(64) NULL,
                   ParentText varchar(1024) NULL
                );
    """
    conn.execute(query)

    return conn


def fetch_comic_info(id):
    url = 'http://xkcd.com/'
    if not id:
        url = url + 'info.0.json'
    else:
        url = url + str(id) + '/info.0.json'

    try:
        response = urllib.request.urlopen(url)
        data = json.load(response)
        data['num'] = int(data['num'])
        return data
    except urllib.error.HTTPError:
        logger.info('Comic %i requested but does not exist', id)
        return {}


def max_comic_id(conn):
    query = "SELECT max(Comic) as max_id FROM comics"

    try:
        cursor = conn.cursor()
        result = cursor.execute(query).fetchone()
        return result[0]
    except Exception:
        logger.info('Could not select max comic id, returning 1')
        return 1


def insert_comic(conn, comic):
    logger.debug(f'Inserting comic {comic}')

    query = """INSERT INTO comics (
                   Comic,
                   ImageName,
                   Title,
                   Text,
                   Transcript
               )
               VALUES (
                   :num,
                   :imgshort,
                   :title,
                   :alt,
                   :transcript
               )
    """
    comic['imgshort'] = comic['img']
    for s in comic['img'].split('/'):
        if '.png' in s or '.jpg' in s:
            comic['imgshort'] = s
            break
    cursor = conn.cursor()
    try:
        cursor.execute(query, comic)
        conn.commit()
    except sqlite3.Error:
        logger.warning('Comic %i already in db', comic['num'])


def generate_comic_list(conn):
    exists_query = "SELECT * FROM comics LIMIT 1"
    try:
        result = conn.execute(exists_query)
        link = result.fetchone()
    except sqlite3.Error:
        query = """
            CREATE TABLE IF NOT EXISTS comics (
                Comic INTEGER PRIMARY KEY,
                ImageName varchar(128),
                Title varchar(256),
                Text varchar(1024),
                Transcript varchar(2048)
            )
        """
        conn.execute(query)

    # Query xckd to get all links
    latest_comic = fetch_comic_info(None)
    latest_id = max_comic_id(conn)

    logger.info('Last comic id seen was %i and the latest is %i', latest_comic['num'], latest_id)

    if latest_id < latest_comic['num']:
        for i in range(latest_id+1, latest_comic['num']):
            comic = fetch_comic_info(i)
            if 'num' in comic:
                insert_comic(conn, comic)


def save_reference(db, reference):
    if 'ParentId' not in reference:
        reference['ParentId'] = None
    if 'ParentText' not in reference:
        reference['ParentText'] = None

    query = """INSERT INTO
               links (
                   CommentId,
                   Text,
                   Link,
                   Comic,
                   ParentId,
                   ParentText
               )
               VALUES (
                   :CommentId,
                   :Text,
                   :Link,
                   :Comic,
                   :ParentId,
                   :ParentText);
               """
    cursor = db.cursor()
    cursor.execute(query, reference)
    db.commit()
