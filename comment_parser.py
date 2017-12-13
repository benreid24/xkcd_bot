import logging
import re

import datastore

logger = logging.getLogger(__name__)


def reference_is_unique(references, comic):
    return comic not in [ref['Comic'] for ref in references]


def contains_reference(comment):
    return 'xkcd.com' in comment and\
           'explainxkcd.com' not in comment and\
           'what-if.xkcd.com' not in comment and\
           'blog.xkcd.com' not in comment


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
    references = []

    for word in comment.split():
        if contains_reference(word):
            sects = re.findall(r'\(([^)]*)\)', word)
            if sects:
                if len(sects)>1:
                    logger.warning(f'Link ({word}) has weird formatting. Taking first bracket region as the url')
                word = sects[0]

            reference = {
                'Text': comment,
                'Link': word,
            }
            comic = parse_link(conn, word)
            if comic != 0 and reference_is_unique(references, comic):
                logger.info('Parsed out link (%s) and id(%i) in xkcd reference', reference['Link'], comic)
                reference['Comic'] = comic
                references.append(reference)

    return references
