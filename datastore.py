import sqlite3
import os
import logging
import threading

logger = logging.getLogger(__name__)
db_lock = threading.Lock()

CREATE_REFERENCE_TABLE_QUERY = """CREATE TABLE IF NOT EXISTS mentions (
                                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                                      Type varchar(16) NOT NULL,
                                      Poster varchar(128) NOT NULL,
                                      Subreddit varchar(128) NOT NULL,
                                      CommentId varchar(64) NOT NULL,
                                      Text varchar(1024) NOT NULL,
                                      Link varchar(256) NOT NULL,
                                      Comic int NOT NULL,
                                      ParentId varchar(64) NULL,
                                      ParentText varchar(1024) NULL,
                                      UNIQUE(CommentId, Comic) ON CONFLICT IGNORE
                                  );
"""

INSERT_REFERENCE_QUERY = """INSERT INTO mentions (
                                Type,
                                Poster,
                                Subreddit,
                                CommentId,
                                Text,
                                Link,
                                Comic,
                                ParentId,
                                ParentText
                            )
                            VALUES (
                                :Type,
                                :Poster,
                                :Sub,
                                :CommentId,
                                :Text,
                                :Link,
                                :Comic,
                                :ParentId,
                                :ParentText
                            );
"""


def connect_datastore(empty=False):
    if empty:
        os.remove('database.db')
    conn = sqlite3.connect('database.db', check_same_thread=False)
    conn.execute(CREATE_REFERENCE_TABLE_QUERY)

    return conn


def comic_id_from_image(conn, img):
    query = 'SELECT Comic FROM comics WHERE ImageName=:img'

    try:
        result = conn.execute(query, {'img': img})
        return result.fetchone()[0]
    except Exception as err:
        logger.debug(f'Failed to get comic id from image file: {err}')
        return 0


def save_reference(db, reference):
    db_lock.acquire()

    if 'ParentId' not in reference:
        reference['ParentId'] = None
    if 'ParentText' not in reference:
        reference['ParentText'] = None

    try:
        cursor = db.cursor()
        cursor.execute(INSERT_REFERENCE_QUERY, reference)
        db.commit()
    except sqlite3.Error as err:
        logger.warning(
            'Error inserting reference %s in db, it likely already exists: %s', reference['CommentId'], str(err)
        )

    db_lock.release()
