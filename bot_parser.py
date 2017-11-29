

def contains_reference(comment):
    return 'xkcd.com' in comment


def parse_link(link):
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

    for word in comment.split():
        if contains_reference(word):
            parsed['Link'] = word
            parsed['Comic'] = parse_link(word)

    return parsed