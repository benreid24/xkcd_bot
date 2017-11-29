import sqlite3
import os

def connect_datastore(empty=False):
    if empty:
        os.remove('database.db')
    conn = sqlite3.connect('database.db')

    query = """CREATE TABLE IF NOT EXISTS links
                (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   CommentId varchar(64) NOT NULL,
                   Text varchar(1024) NOT NULL,
                   Link varchar(256) NOT NULL,
                   Comic int NOT NULL,
                   ParentId varchar(64) NULL,
                   ParentText varchar(1024) NULL
                );
    """
    conn.execute(query)

    return conn


def save_reference(db, reference):
    if 'ParentId' not in reference:
        reference['ParentId'] = None
    if 'ParentText' not in reference:
        reference['ParentText'] = None

    print(reference)

    query = """INSERT INTO
               links (
                   CommentId,
                   Text,
                   Link,
                   Comic,
                   ParentId,
                   ParentText
               )
               VALUES (
                   :CommentId,
                   :Text,
                   :Link,
                   :Comic,
                   :ParentId,
                   :ParentText);
               """
    cursor = db.cursor()
    cursor.execute(query, reference)
    db.commit()
