import json
import logging
import logging.config


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
