import logging
import html
import threading

from praw.models import MoreComments
import praw

import comment_parser as parser
import datastore
import util

logger = logging.getLogger(__name__)


def do_reply(parent, db, references):
    if len(references)==1:
        reply = util.construct_reply(db, references[0]['Comic'])
        try:
            parent.reply(reply)
        except praw.exceptions.APIException:
            logger.warning('Unable to comment on reference to %s', parent.fullname)
            pass


def handle_references(db, references, c_type, poster, sub, comment_id, parent_id, parent_text):
    if references:
        for reference in references:
            reference['Type'] = c_type
            reference['Poster'] = poster
            reference['Sub'] = sub
            reference['CommentId'] = comment_id
            reference['ParentId'] = parent_id
            reference['ParentText'] = parent_text
            datastore.save_reference(db, reference)
    else:
        logger.info(f'Comment {comment_id} did not contain an identifiable reference')


def handle_submission(db, submission):
    poster = submission.author.name
    sub = submission.subreddit.display_name
    body = html.unescape(submission.selftext)
    url = html.unescape(submission.url)
    text = body+"\n"+url

    if parser.contains_reference(text):
        logger.info('Found potential xkcd reference(s) in submission %s', submission.fullname)
        references = parser.parse_comment(db, text)
        handle_references(db, references, 'Submission', poster, sub, submission.fullname, None, None)
        do_reply(submission, db, references)

    comments = submission.comments
    comments.replace_more()
    for comment in submission.comments:
        handle_comment(db, comment, text);


def handle_comment(db, comment, parent_text):
    if isinstance(comment, MoreComments):
        return

    poster = '[deleted]'
    if comment.author is not None:
        poster = comment.author.name
    body = html.unescape(comment.body)
    sub = comment.subreddit.display_name
    parent_id = comment.parent_id

    if parser.contains_reference(body):
        logger.info('Found potential xkcd reference in comment %s', comment.fullname)
        references = parser.parse_comment(db, body)
        handle_references(db, references, 'Comment', poster, sub, comment.fullname, parent_id, parent_text)
        do_reply(comment, db, references)

    replies = comment.replies
    replies.replace_more()
    for reply in replies:
        handle_comment(db, reply, comment.body)


def read_comment_stream(db, subreddit):
    for comment in subreddit.stream.comments():
        handle_comment(db, comment, None)


def read_submission_stream(db, subreddit):
    for submission in subreddit.stream.submissions():
        handle_submission(db, submission)


def run(reddit, db):
    logger.info('Beginning execution of xkcd bot')

    subreddit = reddit.subreddit('all')

    thread = threading.Thread(target=read_submission_stream, args=(db, subreddit))
    thread.start()

    read_comment_stream(db, subreddit)
