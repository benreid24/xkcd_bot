import json
import logging
import logging.config
import html
import os

import xkcd_updater
import datastore

REPLY_TEMPLATE =\
"""[Comic]({link})

[Image]({image})

[Mobile]({mlink})

**Title**: {title}

**Title-text**: {text}

[Explanation]({explainlink})

**Stats**: This comic has previously been referenced {count} times, {stddev:.4f} standard deviations different from the mean ({mean:.4f} refs/comic)
______
[xkcd.com](https://xkcd.com) | [xkcd sub](https://np.reddit.com/r/xkcd) | [Problems/Suggestions](https://github.com/benreid24/xkcd_bot/issues) | [**The stats!**](http://xkcdredditstats.com)
"""


def setup_logging(filename='logging.json'):
    try:
        os.mkdir('logs')
    except FileExistsError:
        pass
    try:
        os.mkdir('data_logs')
    except FileExistsError:
        pass
    with open(filename) as f:
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

    comic_link = f'https://xkcd.com/{comic_id}'
    image_link = comic_info['img']
    mobile_link = f'https://m.xkcd.com/{comic_id}'
    title = html.enescape(comic_info['title'])
    text = html.unescape(comic_info['alt'])
    explainlink = f'https://www.explainxkcd.com/wiki/index.php/{comic_id}'
    refs = stats['ReferenceCount']
    stddevs = stats['StdDevs']
    mean = stats['Mean']

    reply = REPLY_TEMPLATE.format(
        link=comic_link,
        image=image_link,
        mlink=mobile_link,
        title=title,
        text=text,
        explainlink=explainlink,
        count=refs,
        stddev=stddevs,
        mean=mean
    )
    return reply
