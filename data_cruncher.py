import json
import logging

import praw
from sqlalchemy.sql import text

import datastore
import parent_backfiller
import util

logger = logging.getLogger(__name__)


def run(reddit, db):
    logger.info('Backfilling ParentText in mentions table')
    parent_backfiller.run(reddit, db)


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
