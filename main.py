import json
import logging
import os
import smtplib
from email.mime.text import MIMEText

import praw

import datastore as ds
import reference_scanner
import xkcd_updater
import util


def main():
    try:
        # Setup logging
        util.setup_logging()
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
        tunnel = ds.create_ssh_tunnel(
            auth['database']['host'],
            int(auth['database']['port']),
            auth['database']['ssh_user'],
            auth['database']['ssh_pw']
        )

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

        msg = MIMEText(f'Forgive me creator, for I have been slain. My death was caused by:\n\n{err}')
        msg['Subject'] = 'xkcd_stats_bot has died'
        msg['From'] = 'xkcd_stats_bot@xkcdredditstats.com'
        msg['To'] = 'benreid@buffalo.edu'
        s = smtplib.SMTP('localhost')
        s.send_message(msg)
        s.quit()

        os.kill(os.getpid(), 9)

    tunnel.stop()


if __name__ == '__main__':
    main()
