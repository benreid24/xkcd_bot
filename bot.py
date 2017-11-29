import logging

import praw

import parser
import datastore

logger = logging.getLogger(__name__)


def run(reddit, db, config):
    # Run!
    logger.info('Beginning execution of xkcd bot on %i subreddits', len(config['subreddits']))