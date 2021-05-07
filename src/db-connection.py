# -*- coding: utf-8 -*-

import logging
import pandas as pd
from tqdm import tqdm

from py2neo import Graph, SystemGraph, Node, Relationship
from py2neo.database.work import ClientError
from py2neo.client import BrokenTransactionError

from constants import ADMIN_NAME, ADMIN_PASS

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

system_graph = SystemGraph(
    'bolt://localhost:7687',
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
        'bolt://localhost:7687',
        auth=(ADMIN_NAME, ADMIN_PASS),
        name=db_name
    )

    tx = conn.begin()

    person_df = pd.read_csv(
        '../data/person.csv',
        dtype=str,
    )

    for id, name, email in tqdm(person_df[['ID', 'Contact', 'email']].values, desc='Adding personal info'):
        person_node = Node('Person', name=name, id=id)
        tx.create(person_node)

        email_node = Node('Data', email="mailto:john@example.com")
        tx.create(email_node)

        orcid_node = Node('ORCID', orcid='https://orcid.org/0000-0002-7683-0452')  # split node to view only number
        tx.create(orcid_node)

        person_attr_1 = Relationship(person_node, 'has_email', email_node)
        perosn_attr_2 = Relationship(person_node, 'has_orcid', orcid_node)
        tx.create(person_attr_1)
        tx.create(perosn_attr_2)

    institute_df = pd.read_csv(
        '../data/institutes.csv',
        dtype=str
    )

    for id, name in tqdm(institute_df.values, desc='Adding institute information'):
        institute_node = Node('Institute', id=id, name=name)
        tx.create(institute_node)

    for person, institute_id in tqdm(person_df[['ID', '']])

    tx.commit()

    # Creates institute nodes
    create_institute = """
    LOAD CSV WITH HEADERS FROM 'file:///institutes.csv' AS row
    CREATE (i:Institute {id: toInteger(row.ID),
    name: row.Institute})
    """
    conn.query(create_institute, db='test')

    # # Create project nodes
    # create_project = """
    # LOAD CSV WITH HEADERS FROM 'file:///competences.csv' AS row
    # CREATE (p:Project {id: toInteger(row.ID),
    # name: row.Project})
    # """
    # conn.query(create_project, db='test')
    #
    # # Add Project -> Institute edges
    # for i in range(1, 4):
    #     add_institute_data = "LOAD CSV WITH HEADERS FROM 'file:///competences.csv' AS row " + \
    #                          f"WITH row where row.Focus{i} is not null " + \
    #                          "MATCH (p:Project {id: toInteger(row.ID)})," + \
    #                          "(i:Institute {id: toInteger(" + f"row.Focus{i})" + "}) " + \
    #                          "CREATE (i)-[w:PROJECT {name:" + f"'Focus{i}'" + "}]->(p)"
    #     conn.query(add_institute_data, db='test')
    #
    # # Add Institute -> Person data
    # add_person = """
    # LOAD CSV WITH HEADERS FROM 'file:///group.csv' AS row
    # MATCH (p:Person {id: toInteger(row.personID)}),
    # (d:Project {id: toInteger(row.projectID)})
    # CREATE (d)-[:PEOPLE]->(p)
    # """
    # conn.query(add_person, db='test')
    #
    # # Add person meta data such as skills and pathogens
    #
    # create_skills = """
    # LOAD CSV WITH HEADERS FROM 'file:///skills.csv' AS row
    # FIELDTERMINATOR ';'
    # CREATE (s:Skill {id: toInteger(row.ID),
    # skill: row.Skill})
    # """
    # conn.query(create_skills, db='test')
    #
    # create_pathogens = """
    # LOAD CSV WITH HEADERS FROM 'file:///pathogens.csv' AS row
    # CREATE (b:Bacteria {id: toInteger(row.ID),
    # pathogen: row.Pathogens})
    # """
    # conn.query(create_pathogens, db='test')
    #
    # # Person -> Skill
    # for i in range(1, 7):
    #     add_skill_data = "LOAD CSV WITH HEADERS FROM 'file:///person.csv' AS row " + \
    #                      f"WITH row where row.Skill_{i} is not null " + \
    #                      "MATCH (p:Person {id: toInteger(row.ID)})," + \
    #                      "(s:Skill {id: toInteger(" + f"row.Skill_{i})" + "})" + \
    #                      "CREATE (p)-[:SKILLS]->(s)"
    #     conn.query(add_skill_data, db='test')
    #
    # # Person -> Pathogen
    # for i in range(1, 20):
    #     add_bacterial_data = "LOAD CSV WITH HEADERS FROM 'file:///person.csv' AS row " + \
    #                          f"WITH row where row.Pathogen_{i} is not null " + \
    #                          "MATCH (p:Person {id: toInteger(row.ID)})," + \
    #                          "(b:Bacteria {id: toInteger(" + f"row.Pathogen_{i})" + "})" + \
    #                          "CREATE (p)-[:PATHOGEN {name: 'Specializes_in'}]->(b)"
    #
    #     conn.query(add_bacterial_data, db='test')


if __name__ == '__main__':
    create_new_user()
    populate_db(db_name='test')
