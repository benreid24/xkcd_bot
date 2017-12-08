import json
import logging
import logging.config

import praw

import datastore as ds
import bot


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
        reddit = praw.Reddit(username=auth['username'],
                             password=auth['password'],
                             client_id=auth['app_id'],
                             client_secret=auth['secret'],
                             user_agent='xkcd Stats Bot by u/xkcd_stats_bot'
                             )
        logger.info("Connected to Reddit as: "+str(reddit.user.me()))

        # Connect to datastore
        datastore = ds.connect_datastore()
        ds.generate_comic_list(datastore)

        # Run bot
        bot.run(reddit, datastore)

    except Exception as err:
        logger.error('Caught exception: %s', str(err), exc_info=True)


if __name__ == '__main__':
    main()
