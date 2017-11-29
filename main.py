import json
import logging, logging.config

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
        config = json.load(open('config.json'))
        reddit = praw.Reddit(username=config['auth']['username'],
                             password=config['auth']['password'],
                             client_id=config['auth']['app_id'],
                             client_secret=config['auth']['secret'],
                             user_agent='xkcd Stats Bot by u/xkcd_stats_bot'
                             )
        logger.info("Connected to Reddit as: "+str(reddit.user.me()))

        # Connect to datastore
        datastore = ds.connect_datastore()
        bot.run(reddit,datastore)
    except Exception as err:
        logger.error('Caught exception: %s', str(err))


if __name__ == '__main__':
    main()
