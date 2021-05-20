# -*- coding: utf-8 -*-

import logging

from py2neo import Graph, SystemGraph
from py2neo.errors import ClientError
from py2neo.database import Transaction

from constants import ADMIN_NAME, ADMIN_PASS, URL

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

system_graph = SystemGraph(
    URL,
    auth=(ADMIN_NAME, ADMIN_PASS)
)


def create_new_user():
    """Creates a new user with public roles in the DB."""

    print('SUCCESS: Connected to the Neo4j Database.')

    # TODO: Get name list from the survey sheet

    for name in ['test']:
        try:
            system_graph.run(
                """CALL dbms.security.createUser("{}", "pass");""".format(name)
            )
            logger.info(f'Added new user {name}')
        except ClientError:
            logger.info('User exists!')


def create_new_db(db_name: str):
    """Create a new DB table"""

    system_graph.run(
        """CREATE OR REPLACE DATABASE {}""".format(db_name)
    )

    logger.info(f"Created {db_name} in Neo4J!")


def check_database(db_name: str) -> bool:
    database_info = system_graph.run("""SHOW DATABASES""").data()

    for info in database_info:
        if info['name'] == db_name:
            return True
    return False


def populate_db(db_name: str):

    in_db = check_database(db_name)

    if not in_db:
        create_new_db(db_name)
        in_db = check_database(db_name)  # to update the database

    assert in_db is True

    conn = Graph(
        URL,
        auth=(ADMIN_NAME, ADMIN_PASS),
        name=db_name
    )

    tx = conn.begin()
    return tx


def commit(
    db_name: str,
    tx: Transaction
):
    conn = Graph(
        URL,
        auth=(ADMIN_NAME, ADMIN_PASS),
        name=db_name
    )
    conn.commit(tx)

