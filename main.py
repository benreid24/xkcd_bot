import json
import logging
import logging.config

import praw
from sshtunnel import SSHTunnelForwarder

import datastore as ds
import reference_scanner
import xkcd_updater


def setup_logging():
    with open('logging.json') as f:
        config = json.load(f)
        logging.config.dictConfig(config)


def main():
    try:
        # Setup logging
        setup_logging()
        logger = logging.getLogger(__name__)

        # Connect to Reddit
        auth = json.load(open('auth.json'))
        reddit = praw.Reddit(username=auth['reddit']['username'],
                             password=auth['reddit']['password'],
                             client_id=auth['reddit']['app_id'],
                             client_secret=auth['reddit']['secret'],
                             user_agent=auth['reddit']['user_agent']
                             )
        logger.info("Connected to Reddit as: "+str(reddit.user.me()))

        # SSH tunnel to database
        tunnel = SSHTunnelForwarder(
            (auth['database']['host'], int(auth['database']['port'])),
            ssh_username=auth['database']['ssh_user'],
            ssh_password=auth['database']['ssh_pw'],
            remote_bind_address=('127.0.0.1', 3306)
        )
        tunnel.start()

        # Connect to datastore
        db = ds.connect_datastore(
            '127.0.0.1',
            tunnel.local_bind_port,
            auth['database']['name'],
            auth['database']['user'],
            auth['database']['password']
        )

        # Check once for new comics before streaming new data forever
        xkcd_updater.run(db)

        # Run bot
        reference_scanner.run(reddit, db)

    except Exception as err:
        logger.error('Caught exception: %s', str(err), exc_info=True)

    tunnel.stop()


if __name__ == '__main__':
    main()
