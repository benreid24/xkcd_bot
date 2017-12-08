# xkcd_bot
Basic bot to track xkcd links on Reddit and calculate general stats
Inspired by the xkcd_transcriber_bot on Reddit (https://www.reddit.com/user/xkcd_transcriber), this bot is a replica that will
hopefully replace the now-offline xkcd_transcriber_bot

## Running the bot
Currently the bot is runnning Python 3.6.3, but this may change depending on what version my hosting supports. The bot requires
some data to authenticate to Reddit and the archive database. This data should be placed into a file `auth.json` in the root folder
of the bot. It looks like this:

```
{
  "reddit": {
    "user_agent": "Your user agent",
    "username": "Reddit username",
    "password": "Reddit password",
    "app_id": "Reddit app id",
    "secret": "Reddit app secret"
  },
  "database": {
    "host": "Database remote host name",
    "port": "Database remote port for ssh tunnel",
    "user": "Database username",
    "password": "Database password",
    "name": "Database name",
    "ssh_user": "SSH username for tunnel",
    "ssh_pw": "SSH password for tunnel"
  }
}
```

Once you create the config file, simply install the packages in `requirements.txt` and run `main.py` to begin data collection. The
following data is collected:

| Type                  | Poster      | Subreddit     | CommentId         | CommentText      | Link           | Comic    | ParentId             | ParentText             |
|-----------------------|-------------|---------------|-------------------|------------------|----------------|----------|----------------------|------------------------|
| Comment or submission | Reddit user | The subreddit | Id of the comment | The comment test | Full xkcd link | Comic id | Id of parent comment | Text of parent comment |

Because the bot fetches new data in a stream format, it generally will not populate the ParentText column. To counter this, and to calculate
additional statistics, there is another module called `data_cruncher.py` that backfills the ParentText column where necessary, as well as
calculates statistsics on the references found. These stats are stored in different tables (coming soon) that will eventually be used by the
web interface
