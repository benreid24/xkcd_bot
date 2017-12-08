import logging
import sqlite3
import urllib
import urllib.request
import json

import datastore

logger = logging.getLogger(__name__)

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

    latest_comic = fetch_comic_info(None)
    latest_id = max_comic_id(conn)

    logger.info('Last comic id seen was %i and the latest is %i', latest_id, latest_comic['num'])

    if latest_id < latest_comic['num']:
        for i in range(latest_id+1, latest_comic['num']+1):
            logger.info('Fetching comic: %i', i)
            comic = fetch_comic_info(i)
            if 'num' in comic:
                insert_comic(conn, comic)


def run():
    db = datastore.connect_datastore()
    generate_comic_list(db)


if __name__ == '__main__':
    run()
