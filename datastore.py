import sqlite3
import os
import urllib.request
import json
import logging

logger = logging.getLogger(__name__)

CREATE_REFERENCE_TABLE_QUERY = """CREATE TABLE IF NOT EXISTS mentions (
                                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                                      Poster varchar(128) NOT NULL,
                                      Subreddit varchar(128) NOT NULL,
                                      CommentId varchar(64) NOT NULL,
                                      Text varchar(1024) NOT NULL,
                                      Link varchar(256) NOT NULL,
                                      Comic int NOT NULL,
                                      ParentId varchar(64) NULL,
                                      ParentText varchar(1024) NULL,
                                      UNIQUE(CommentId, Comic) ON CONFLICT IGNORE
                                  );
"""

INSERT_REFERENCE_QUERY = """INSERT INTO mentions (
                                Poster,
                                Subreddit,
                                CommentId,
                                Text,
                                Link,
                                Comic,
                                ParentId,
                                ParentText
                            )
                            VALUES (
                                :Poster,
                                :Sub,
                                :CommentId,
                                :Text,
                                :Link,
                                :Comic,
                                :ParentId,
                                :ParentText
                            );
"""

CREATE_COMIC_TABLE_QUERY = """CREATE TABLE IF NOT EXISTS comics (
                                  Comic INTEGER PRIMARY KEY,
                                  ImageName varchar(128),
                                  Title varchar(256),
                                  Text varchar(1024),
                                  Transcript varchar(2048)
                              );
"""

INSERT_COMIC_QUERY = """INSERT INTO comics (
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
                        );
"""


def connect_datastore(empty=False):
    if empty:
        os.remove('database.db')
    conn = sqlite3.connect('database.db')
    fetch_comic_info(None)

    conn.execute(CREATE_REFERENCE_TABLE_QUERY)

    return conn


def comic_id_from_image(conn, img):
    query = 'SELECT Comic FROM comics WHERE ImageName=:img'

    try:
        result = conn.execute(query, {'img': img})
        return result.fetchone()[0]
    except Exception:
        return 0


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
        if result[0] is None:
            return 0
        return result[0]
    except sqlite3.Error:
        logger.info('Could not select max comic id, returning 1')
        return 0


def insert_comic(conn, comic):
    logger.debug(f'Inserting comic {comic}')

    comic['imgshort'] = comic['img']
    for s in comic['img'].split('/'):
        if '.png' in s or '.jpg' in s:
            comic['imgshort'] = s
            break
    cursor = conn.cursor()
    try:
        cursor.execute(INSERT_COMIC_QUERY, comic)
        conn.commit()
    except sqlite3.Error as err:
        logger.warning('Comic %i already in db: %s', comic['num'], str(err))


def generate_comic_list(conn):
    conn.execute(CREATE_COMIC_TABLE_QUERY)

    # Query xckd to get all links
    latest_comic = fetch_comic_info(None)
    latest_id = max_comic_id(conn)

    logger.info('Last comic id seen was %i and the latest is %i', latest_id, latest_comic['num'])

    if latest_id < latest_comic['num']:
        for i in range(latest_id+1, latest_comic['num']+1):
            logger.info('Fetching comic: %i', i)
            comic = fetch_comic_info(i)
            if 'num' in comic:
                insert_comic(conn, comic)


def save_reference(db, reference):
    if 'ParentId' not in reference:
        reference['ParentId'] = None
    if 'ParentText' not in reference:
        reference['ParentText'] = None

    try:
        cursor = db.cursor()
        cursor.execute(INSERT_REFERENCE_QUERY, reference)
        db.commit()
    except sqlite3.Error as err:
        logger.warning(
            'Error inserting reference %s in db, it likely already exists: %s', reference['CommentId'], str(err)
        )
