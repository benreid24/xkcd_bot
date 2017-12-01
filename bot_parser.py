import logging

import datastore

logger = logging.getLogger(__name__)


def contains_reference(comment):
    # TODO Ensure that it is actually a comic being referenced, not a whatif
    return 'xkcd.com' in comment and 'explainxkcd.com' not in comment


def parse_link(conn, link):
    for part in link.split('/'):
        if '.jpg' in part or '.png' in part:
            comic = datastore.comic_id_from_image(conn, part)
            if comic != 0:
                return comic
        try:
            comic = int(part)
            return comic
        except ValueError:
            continue
    return 0


def parse_comment(conn, comment):
    parsed = {
        'Text': comment
    }

    # TODO What do we do if there are multiple comics referenced?
    for word in comment.split():
        if contains_reference(word):
            parsed['Link'] = word # TODO Remove Reddit link syntax around the link, if present
            parsed['Comic'] = parse_link(conn, word)
            logger.info('Parsed out link (%s) and id(%i) in xkcd reference', parsed['Link'], parsed['Comic'])

    return parsed
