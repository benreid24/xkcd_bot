import json
import logging
import logging.config


def setup_logging():
    with open('logging.json') as f:
        config = json.load(f)
        logging.config.dictConfig(config)


def result_proxy_to_dict_list(results):
    return [dict(row) for row in results]
