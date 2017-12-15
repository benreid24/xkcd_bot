import json
import logging
import datetime
import csv
import os
import zipfile

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
                                      Percent double NOT NULL,
                                      NormPercent double NULL,
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


def group_subs(references):
    subs = {}

    for reference in references:
        if reference['Subreddit'] not in subs:
            subs[reference['Subreddit']] = {'Count': 1}
        else:
            subs[reference['Subreddit']]['Count'] += 1
    return subs


def calc_sub_percents(subs, total_refs):
    sig_subs = []
    sig_refs = 0
    for sub in subs.keys():
        subs[sub]['NormPercent'] = None
        subs[sub]['Percent'] = subs[sub]['Count']/total_refs*100
        if subs[sub]['Percent'] > 1:
            sig_subs.append(sub)
            sig_refs += subs[sub]['Count']
    for sub in sig_subs:
        subs[sub]['NormPercent'] = subs[sub]['Count']/sig_refs*100
    return (total_refs-sig_refs)/total_refs*100


def add_comic_stats(comic_group, total_refs, mean, std_dev):
    comic_list = []

    for comic in comic_group:
        data = {
            'Comic': comic,
            'Count': comic_group[comic],
            'Percent': comic_group[comic] / total_refs * 100,
            'StdDevsFromMean': (comic_group[comic] - mean) / std_dev
        }
        comic_list.append(data)

    return comic_list


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


def save_comic_counts(db, comic_group):
    query = text("""INSERT INTO comic_counts (Comic, ReferenceCount, Percent, StdDevs)
                    VALUES(:Comic, :Count, :Percent, :StdDevsFromMean)
                    ON DUPLICATE KEY UPDATE ReferenceCount=:Count, Percent=:Percent, StdDevs=:StdDevsFromMean
                 """)

    for comic in comic_group:
        db.execute(query, comic)


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


def save_sub_counts(db, subs):
    query = text("""INSERT INTO subreddits (Name, ReferenceCount, Percent, NormPercent)
                    VALUES(:sub, :refs, :pc, :npc) ON DUPLICATE KEY UPDATE
                    ReferenceCount=:refs, Percent=:pc, NormPercent=:npc
                 """)

    for sub in subs:
        params = {
            'sub': sub,
            'refs': subs[sub]['Count'],
            'pc': subs[sub]['Percent'],
            'npc': subs[sub]['NormPercent']
        }
        db.execute(query, params)


def count_nonzero_comics(refs):
    count = 0
    for key in refs.keys():
        if refs[key] > 0:
            count += 1
    return count


def get_bot_age():
    start = datetime.datetime(year=2017, month=12, day=13, hour=17, tzinfo=datetime.timezone.utc)
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    diff = now - start
    return diff.total_seconds()/3600


def sort_dict_list(data, sort_col):
    return sorted(data, key=lambda k: k[sort_col], reverse=True)


def add_rank_column(data):
    rank = 1
    for row in data:
        row['Rank'] = rank
        rank += 1


def save_to_csv(file, data):
    if len(data) == 0:
        return

    keys = list(data[0].keys())
    if 'Rank' in keys:
        keys.remove('Rank')
        keys.insert(0, 'Rank')
    with open(file, 'w', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(data)


def zipdir(path, ziph):
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))


def save_raw_data(db, comic_ranking):
    try:
        os.mkdir('data')
    except FileExistsError:
        pass
    save_to_csv('data/ranking.csv', comic_ranking)

    db_tables = [
        'mentions',
        'comic_counts',
        'poster_counts',
        'comics',
        'stats',
        'subreddits'
    ]
    for table in db_tables:
        data = datastore.fetch_all_data(db, table)
        save_to_csv(f'data/{table}.csv', data)

    zipf = zipfile.ZipFile('rawdata.zip', 'w', zipfile.ZIP_DEFLATED)
    zipdir('data/', zipf)
    zipf.close()


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
    grouped_subs = group_subs(references)

    stats = compute_basic_stats(references, grouped_comics)
    stats['UniquePosters'] = len(grouped_posters)
    stats['UniqueComics'] = count_nonzero_comics(grouped_comics)
    stats['UniqueSubs'] = len(grouped_subs)
    stats['RefsPerHour'] = stats['TotalReferences']/get_bot_age()
    stats['LessOneRefSubs'] = calc_sub_percents(grouped_subs, stats['TotalReferences'])

    grouped_comics = add_comic_stats(grouped_comics,
                                     stats['TotalReferences'],
                                     stats['AverageReferencesPerComic'],
                                     stats['ComicReferenceCountStdDev'])

    logger.info('Saving computed stats')
    save_comic_counts(db, grouped_comics)
    save_poster_counts(db, grouped_posters)
    save_sub_counts(db, grouped_subs)
    save_stats(db, stats)

    logger.info('Saving raw data')
    grouped_comics = sort_dict_list(grouped_comics, 'Count')
    add_rank_column(grouped_comics)
    save_raw_data(db, grouped_comics)

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
