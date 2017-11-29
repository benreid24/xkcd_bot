import sqlite3
import os

def connect_datastore(empty=False):
    if empty:
        os.remove('database.db')
    conn = sqlite3.connect('database.db')

    query = """CREATE TABLE IF NOT EXISTS links
                (
                   id INTEGER PRIMARY KEY,
                   Poster varchar(128),
                   Text varchar(1024),
                   Link varchar(256),
                   Comic int,
                   ParentText varchar(1024)
                );
    """
    conn.execute(query)

    return conn
