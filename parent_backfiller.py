import html
import logging

from sqlalchemy.sql import text

import util

logger = logging.getLogger(__name__)


def select_comments_no_parent_text(db):
    query = "SELECT id, ParentId FROM mentions WHERE Type='Comment' AND ParentText IS NULL"
    results = db.execute(query)
    return util.result_proxy_to_dict_list(results)


def add_parent_text(reddit, reference):
    parent_id = reference['ParentId']
    if 't1_' in parent_id:
        comment = reddit.comment(id=parent_id[3:])
        reference['ParentText'] = html.unescape(comment.body)
    else:
        submission = reddit.submission(id=parent_id[3:])
        body = html.unescape(submission.selftext)
        url = html.unescape(submission.url)
        reference['ParentText'] = body + "\n" + url


def save_parent_text(db, reference):
    query = text('UPDATE mentions SET ParentText=:ParentText WHERE id=:id')
    db.execute(query, reference)


def run(reddit, db):
    rows = select_comments_no_parent_text(db)
    logger.info('Found %i rows with no ParentText', len(rows))
    for row in rows:
        add_parent_text(reddit, row)
        save_parent_text(db, row)