import logging

import util

import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
from sshtunnel import SSHTunnelForwarder

logger = logging.getLogger(__name__)

CREATE_REFERENCE_TABLE_QUERY = """CREATE TABLE IF NOT EXISTS mentions (
                                      id int AUTO_INCREMENT,
                                      Type varchar(16) NOT NULL,
                                      Poster varchar(128) NOT NULL,
                                      Subreddit varchar(128) NOT NULL,
                                      CommentId varchar(64) NOT NULL,
                                      Text varchar(1024) NOT NULL,
                                      Link varchar(256) NOT NULL,
                                      Comic int NOT NULL,
                                      ParentId varchar(64) NULL,
                                      ParentText varchar(1024) NULL,
                                      
                                      PRIMARY KEY(id),
                                      UNIQUE(CommentId, Comic)
                                  );
"""

INSERT_REFERENCE_QUERY = """INSERT IGNORE INTO mentions (
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


def create_ssh_tunnel(host, port, username, password):
    tunnel = SSHTunnelForwarder(
        (host, port),
        ssh_username=username,
        ssh_password=password,
        remote_bind_address=('127.0.0.1', 3306)
    )
    tunnel.start()
    return tunnel


def connect_datastore(db_host, db_port, db_name, sql_user, sql_pw):
    conn_str = f'mysql+cymysql://{sql_user}:{sql_pw}@{db_host}:{db_port}/{db_name}?charset=utf8'
    conn = sqlalchemy.create_engine(conn_str)

    conn.execute(text(CREATE_REFERENCE_TABLE_QUERY))
    conn.execute(f'ALTER DATABASE {db_name} CHARACTER SET utf8 COLLATE utf8_unicode_ci;')

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
    if 'ParentId' not in reference:
        reference['ParentId'] = None
    if 'ParentText' not in reference:
        reference['ParentText'] = None

    try:
        db.execute(text(INSERT_REFERENCE_QUERY), reference)
    except SQLAlchemyError as err:
        logger.warning(
            'Error inserting reference %s in db, it likely already exists: %s', reference['CommentId'], str(err)
        )


def fetch_all_data(db, table):
    proxy = db.execute(f'SELECT * FROM {table}')
    return util.result_proxy_to_dict_list(proxy)
