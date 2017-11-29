import logging

logger = logging.getLogger(__name__)


def contains_reference(comment):
    return 'xkcd.com' in comment # TODO Ensure that it is actually a comic being referenced, not a whatif


def parse_link(link):
    # TODO Find references to image files and replace them with comic ids

    for part in link.split('/'):
        try:
            comic = int(part)
            return comic
        except ValueError:
            continue
    return 0


def parse_comment(comment):
    parsed = {
        'Text': comment
    }

    # TODO What do we do if there are multiple comics referenced?
    for word in comment.split():
        if contains_reference(word):
            parsed['Link'] = word # TODO Remove Reddit link syntax around the link, if present
            parsed['Comic'] = parse_link(word)
            logger.info('Parsed out link (%s) and id(%i) in xkcd reference', parsed['Link'], parsed['Comic'])

    return parsed
