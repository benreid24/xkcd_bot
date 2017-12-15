import json
import logging
import logging.config

import xkcd_updater
import datastore

REPLY_TEMPLATE =\
"""[Image]({image})

[Mobile]({mlink})

**Title**: {title}

**Title-text**: {text}

[Explaination]({explainlink})

**Stats**: This comic has previously been referenced {count} times, {stddev:.4f} standard deviations different from the mean
______
[xkcd.com](https://xkcd.com) | [xkcd sub](https://np.reddit.com/r/xkcd) | [Problems/Suggestions](https://github.com/benreid24/xkcd_bot/issues) | [The stats!](http://xkcdredditstats.com)
"""


def setup_logging():
    with open('logging.json') as f:
        config = json.load(f)
        logging.config.dictConfig(config)


def result_proxy_to_dict_list(results):
    return [dict(row) for row in results]


def calc_std_dev(values):
    avg = calc_mean(values)
    var = 0
    for val in values:
        var += (val-avg)**2
    var /= len(values)
    return var**0.5


def calc_mean(values):
    return sum(values)/len(values)


def construct_reply(db, comic_id):
    comic_info = xkcd_updater.fetch_comic_info(comic_id)
    stats = datastore.get_comic_stats(db, comic_id)

    image_link = comic_info['img']
    mobile_link = f'https://m.xkcd.com/{comic_id}'
    title = comic_info['title']
    text = comic_info['alt']
    explainlink = f'https://www.explainxkcd.com/wiki/index.php/{comic_id}'
    refs = stats['ReferenceCount']
    stddevs = stats['StdDevs']

    reply = REPLY_TEMPLATE.format(
        image=image_link,
        mlink=mobile_link,
        title=title,
        text=text,
        explainlink=explainlink,
        count=refs,
        stddev=stddevs
    )
    return reply
