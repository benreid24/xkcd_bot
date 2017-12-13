import logging
import urllib
import urllib.request
import json
import ssl

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text

import datastore
import util

logger = logging.getLogger(__name__)

CREATE_COMIC_TABLE_QUERY = """CREATE TABLE IF NOT EXISTS comics (
                                  Comic int,
                                  ImageName varchar(128) NOT NULL,
                                  Title varchar(256) NOT NULL,
                                  Text varchar(1024) NOT NULL,
                                  Transcript varchar(8192) NOT NULL,
                                  
                                  PRIMARY KEY(Comic)
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
        context = ssl._create_unverified_context()
        response = urllib.request.urlopen(url, context=context)
        data = json.load(response)
        data['num'] = int(data['num'])
        return data
    except urllib.error.HTTPError:
        logger.info('Comic %i requested but does not exist', id)
        return {}


def max_comic_id(db):
    query = "SELECT max(Comic) as max_id FROM comics"

    try:
        result = db.execute(query).fetchone()
        if result[0] is None:
            return 0
        return result[0]
    except SQLAlchemyError:
        logger.info('Could not select max comic id, returning 1')
        return 0


def insert_comic(db, comic):
    logger.debug(f'Inserting comic {comic}')

    comic['imgshort'] = comic['img']
    for s in comic['img'].split('/'):
        if '.png' in s or '.jpg' in s:
            comic['imgshort'] = s
            break
    try:
        db.execute(text(INSERT_COMIC_QUERY), comic)
    except SQLAlchemyError as err:
        logger.warning('Comic %i already in db: %s', comic['num'], str(err))


def generate_comic_list(db):
    db.execute(text(CREATE_COMIC_TABLE_QUERY))

    latest_comic = fetch_comic_info(None)
    latest_id = max_comic_id(db)

    logger.info('Last comic id seen was %i and the latest is %i', latest_id, latest_comic['num'])

    if latest_id < latest_comic['num']:
        for i in range(latest_id+1, latest_comic['num']+1):
            logger.info('Fetching comic: %i', i)
            comic = fetch_comic_info(i)
            if 'num' in comic:
                insert_comic(db, comic)


def get_comic_ids(db):
    query = "SELECT Comic FROM comics"
    result = db.execute(query)
    ids = util.result_proxy_to_dict_list(result)
    return [comic['Comic'] for comic in ids]


def run(db):
    generate_comic_list(db)


if __name__ == '__main__':
    auth = json.load(open('auth.json'))
    tunnel = datastore.create_ssh_tunnel(
        auth['database']['host'],
        int(auth['database']['port']),
        auth['database']['ssh_user'],
        auth['database']['ssh_pw']
    )
    conn = datastore.connect_datastore(
        '127.0.0.1',
        tunnel.local_bind_port,
        auth['database']['name'],
        auth['database']['user'],
        auth['database']['password']
    )

    run(conn)
    tunnel.stop()
