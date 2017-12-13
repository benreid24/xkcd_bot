import json
import logging

import praw
from sqlalchemy.sql import text

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
                                  Percent double NOT NULL,
                                  StdDevs double NOT NULL,
                                  UNIQUE(Comic)
                              )
"""

USERS_TABLE_CREATE_QUERY = """CREATE TABLE IF NOT EXISTS poster_counts (
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


def group_comics(references, ids):
    comics = {comic: 0 for comic in ids}
    for reference in references:
        if reference['Comic'] in comics:
            comics[reference['Comic']] += 1
        else:
            logger.warning('Reference to nonexistent comic %i', reference['Comic'])
    return comics


def group_posters(references):
    posters = {}

    for reference in references:
        if reference['Poster'] not in posters:
            posters[reference['Poster']] = 1
        else:
            posters[reference['Poster']] += 1
    return posters


def compute_basic_stats(references, comic_group):
    counts = [comic_group[key] for key in comic_group.keys()]
    stats = {
        'TotalReferences': len(references),
        'AverageReferencesPerComic': len(references)/len(comic_group),
        'ComicReferenceCountStdDev': util.calc_std_dev(counts)
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


def save_comic_counts(db, comic_group, total_refs, mean, std_dev):
    query = text("""INSERT INTO comic_counts (Comic, ReferenceCount, Percent, StdDevs)
                    VALUES(:comic, :refs, :percent, :stddevs)
                    ON DUPLICATE KEY UPDATE ReferenceCount=:refs, Percent=:percent, StdDevs=:stddevs
                 """)

    for comic in comic_group.keys():
        params = {
            'comic': comic,
            'refs': comic_group[comic],
            'percent': comic_group[comic]/total_refs*100,
            'stddevs': (comic_group[comic]-mean)/std_dev
        }
        db.execute(query, params)


def save_poster_counts(db, posters):
    query = text("""INSERT INTO poster_counts (Name, ReferenceCount)
                    VALUES(:poster, :refs) ON DUPLICATE KEY UPDATE ReferenceCount=:refs
                 """)

    for poster in posters:
        params = {
            'poster': poster,
            'refs': posters[poster]
        }
        db.execute(query, params)


def run(reddit, db):
    create_tables(db)
    xkcd.run(db)
    xkcd_ids = xkcd.get_comic_ids(db)

    logger.info('Backfilling ParentText in mentions table')
    parent_backfiller.run(reddit, db)

    logger.info('Fetching all xkcd references')
    references = get_all_references(db)

    logger.info('Computing stats')
    grouped_comics = group_comics(references, xkcd_ids)
    grouped_posters = group_posters(references)

    stats = compute_basic_stats(references, grouped_comics)
    stats['UniquePosters'] = len(grouped_posters)
    stats['UniqueComics'] = len(grouped_comics)

    logger.info('Saving computed stats')
    save_comic_counts(db,
                      grouped_comics,
                      stats['TotalReferences'],
                      stats['AverageReferencesPerComic'],
                      stats['ComicReferenceCountStdDev'])
    save_poster_counts(db, grouped_posters)
    save_stats(db, stats)

    logger.info('Done crunching data')


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
