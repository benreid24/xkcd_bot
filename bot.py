import logging
import html
import threading

from praw.models import MoreComments

import bot_parser as parser
import datastore

logger = logging.getLogger(__name__)


def handle_references(db, references, poster, sub, comment_id, parent_id, parent_text):
    if references:
        for reference in references:
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
        handle_references(db, references, poster, sub, submission.fullname, None, None)

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
        handle_references(db, references, poster, sub, comment.fullname, parent_id, parent_text)

    replies = comment.replies
    replies.replace_more()
    for reply in replies:
        handle_comment(db, reply, comment.body)


def read_comment_stream(db, subreddit):
    count = 0
    for comment in subreddit.stream.comments():
        handle_comment(db, comment, None)
        count += 1
        if count >= 5:
            break


def read_submission_stream(db, subreddit):
    count = 0
    for submission in subreddit.stream.submissions():
        handle_submission(db, submission)
        count += 1
        if count >= 5:
            break


def run(reddit, db):
    logger.info('Beginning execution of xkcd bot')

    subreddit = reddit.subreddit('xkcd')
    t1 = threading.Thread(target=read_comment_stream, args=(db, subreddit))
    t1.start()
    t2 = threading.Thread(target=read_submission_stream, args=(db, subreddit))
    t2.start()
