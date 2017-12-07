import logging
import html

import praw

import bot_parser as parser
import datastore

logger = logging.getLogger(__name__)


def handle_references(db, references, comment_id, parent_id, parent_text):
    if references:
        for reference in references:
            reference['CommentId'] = comment_id
            reference['ParentId'] = parent_id
            reference['ParentText'] = parent_text
            datastore.save_reference(db, reference)
    else:
        logger.info(f'Comment {comment_id} did not contain an identifiable reference')


def handle_submission(db, submission):
    body = html.unescape(submission.selftext)
    url = html.unescape(submission.url)
    text = body+"\n"+url

    if parser.contains_reference(text):
        logger.info('Found potential xkcd reference(s) in submission %s', submission.fullname)
        references = parser.parse_comment(db, text)
        handle_references(db, references, submission.fullname, None, None)

    comments = submission.comments
    comments.replace_more(10)
    for comment in submission.comments:
        handle_comment(db, comment, submission.fullname, text);


def handle_comment(db, comment, parent_id, parent_text):
    body = html.unescape(comment.body)

    if parser.contains_reference(body):
        logger.info('Found xkcd reference in comment %s', comment.fullname)
        references = parser.parse_comment(db, body)
        handle_references(db, references, comment.fullname, parent_id, parent_text)

    replies = comment.replies
    replies.replace_more(5)
    for reply in replies:
        handle_comment(db, reply, comment.fullname, comment.body)


def run(reddit, db, config):
    # Run!
    logger.info('Beginning execution of xkcd bot on %i subreddits', len(config['subreddits']))

    subreddit_str = '+'.join(config['subreddits'])

    subreddit = reddit.subreddit(subreddit_str)
    for submission in subreddit.top("all", limit=2):
        handle_submission(db, submission)
