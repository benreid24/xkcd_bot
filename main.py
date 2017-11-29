import json
import logging

import praw

import datastore as ds
import bot

def main():
    #Connect to Reddit
    config = json.load(open('config.json'))
    reddit = praw.Reddit(username=config['auth']['username'],
                         password=config['auth']['password'],
                         client_id=config['auth']['app_id'],
                         client_secret=config['auth']['secret'],
                         user_agent='xkcd Stats Bot by u/xkcd_stats_bot'
                         )
    print(reddit.user.me())

    #Connect to datastore
    datastore = ds.connect_datastore()
    bot.run(reddit,datastore)

if __name__=='__main__':
    main()