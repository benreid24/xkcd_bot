import json
import logging

import praw
from sqlalchemy.sql import text
import numpy

import datastore
import parent_backfiller
import util
import xkcd_updater as xkcd

logger = logging.getLogger(__name__)

STATS_TABLE_CREATE_QUERY = """CREATE TABLE IF NOT EXISTS stats (
                                  Name varchar(32) NOT NULL,
                                  Value double NOT NULL,
                                  UNIQUE(Name)
                              )
"""

COMIC_TABLE_CREATE_QUERY = """CREATE TABLE IF NOT EXISTS comic_counts (
                                  Comic int NOT NULL,
                                  ReferenceCount int NOT NULL,
                                  PRIMARY KEY(Comic)
                              )
"""

USERS_TABLE_CREATE_QUERY = """CREATE TABLE IF NOT EXISTS redditors (
                                  Name varchar(32) NOT NULL,
                                  ReferenceCount int NOT NULL,
                                  UNIQUE(Name)
                              )
"""

SUBREDDIT_TABLE_CREATE_QUERY = """CREATE TABLE IF NOT EXISTS subreddits (
                                      Name varchar(32) NOT NULL,
                                      ReferenceCount int NOT NULL,
                                      UNIQUE(Name)
                                  )
"""


def get_all_references(db):
    query = 'SELECT Type, Poster, Subreddit, Comic FROM mentions'
    results = db.execute(query)
    return util.result_proxy_to_dict_list(results)


def create_tables(db):
    db.execute(STATS_TABLE_CREATE_QUERY)
    db.execute(USERS_TABLE_CREATE_QUERY)
    db.execute(SUBREDDIT_TABLE_CREATE_QUERY)
    db.execute(COMIC_TABLE_CREATE_QUERY)


def group_comics(references, n_comics):
    comics = [0] * n_comics
    for reference in references:
        comics[reference['Comic']] += 1
    return comics


def compute_basic_stats(references, comic_group):

    stats = {
        'TotalReferences': len(references),
        'AverageReferencesPerComic': len(references)/len(comic_group),
        'ComicReferenceCountStdDev': numpy.std(numpy.array(comic_group), ddof=0)
    }
    return stats


def save_stats(db, stats):
    query = text('INSERT INTO stats (Name, Value) VALUES(:name, :val) ON DUPLICATE KEY UPDATE Value=:val')

    for stat in stats:
        params = {
            'name': stat,
            'val': stats[stat]
        }
        db.execute(query, params)


def save_comic_counts(db, comic_group):
    query = text("""INSERT INTO comic_counts (Comic, ReferenceCount)
                    VALUES(:comic, :refs) ON DUPLICATE KEY UPDATE ReferenceCount=:refs
                 """)

    comic_id = 1
    for refs in comic_group:
        params = {
            'comic': comic_id,
            'refs': refs
        }
        db.execute(query, params)
        comic_id += 1


def run(reddit, db):
    create_tables(db)
    xkcd.run(db)

    logger.info('Backfilling ParentText in mentions table')
    parent_backfiller.run(reddit, db)

    logger.info('Fetching all xkcd references')
    references = get_all_references(db)

    max_comic = xkcd.max_comic_id(db)
    grouped_comics = group_comics(references, max_comic)
    stats = compute_basic_stats(references, grouped_comics)

    save_comic_counts(db, grouped_comics)
    save_stats(db, stats)


if __name__ == '__main__':
    util.setup_logging()
    logger = logging.getLogger(__name__)

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
    reddit = praw.Reddit(username=auth['reddit']['username'],
                         password=auth['reddit']['password'],
                         client_id=auth['reddit']['app_id'],
                         client_secret=auth['reddit']['secret'],
                         user_agent=auth['reddit']['user_agent']
                         )
    logger.info("Connected to Reddit as: " + str(reddit.user.me()))

    run(reddit, conn)
    tunnel.stop()
