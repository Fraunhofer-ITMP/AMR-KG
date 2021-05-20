# -*- coding: utf-8 -*-

import getopt
import os
import sys

import pandas as pd
from py2neo import Node, Relationship
from py2neo.database import Transaction
from tqdm import tqdm

from connection import commit, populate_db
from constants import DATA_DIR


def map_data(
        data_df: pd.DataFrame
):
    # Map to institute name
    institute_dict = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'institute.csv'),
        dtype=str,
        index_col='id',
    ).to_dict()['institute']

    data_df['institute'] = data_df['institute'].map(institute_dict)

    # Map to project name
    project_dict = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'project.csv'),
        dtype=str,
        index_col='id',
    ).to_dict()['project']

    for i in ['project_1', 'project_2']:
        data_df[i] = data_df[i].map(project_dict)

    # Map to bacterial strain
    pathogen_dict = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'pathogen.csv'),
        dtype=str,
        index_col='id',
    ).to_dict()['pathogen']

    for i in ['pathogen_1', 'pathogen_2', 'pathogen_3']:
        data_df[i] = data_df[i].map(pathogen_dict)

    # Map to skill set
    skill_dict = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'skill.csv'),
        dtype=str,
        index_col='id',
    ).to_dict()['skill']

    for i in ['skill_1', 'skill_2', 'skill_3', 'skill_4']:
        data_df[i] = data_df[i].map(skill_dict)

    return data_df


def add_nodes(tx: Transaction):
    """Add nodes specific to AMR data """

    node_dict = {
        'Person': {},
        'Institute': {},
        'Skill': {},
        'Pathogen': {},
        'Project': {}
    }

    # Create person nodes
    person_df = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'person.csv'),
        usecols=[
            'contact',
            'email',
            'orcid'
        ]
    )

    for name, email, orcid in person_df.values:
        person_property = {}

        if pd.notna(name):
            person_property['name'] = name

        if pd.notna(email):
            person_property['email'] = email

        if pd.notna(orcid):
            person_property['orcid'] = orcid

        node_dict['Person'][name] = Node('Person', **person_property)
        tx.create(node_dict['Person'][name])

    # Create institute nodes
    institute_df = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'institute.csv'),
        index_col='id',
    )

    for institute_name, institute_page in institute_df.values:
        institute_property = {}

        if pd.notna(institute_name):
            institute_name = institute_name[0]
            institute_property['name'] = institute_name
            institute_property['link'] = institute_page

            node_dict['Institute'][institute_name] = Node('Institute', **institute_property)
            tx.create(node_dict['Institute'][institute_name])

    # Create project nodes
    project_df = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'project.csv'),
        dtype=str,
        index_col='id',
    )

    for project_name in project_df.values:
        project_property = {}

        if pd.notna(project_name):
            project_name = project_name[0]
            project_property['name'] = project_name
            project_property['link'] = f'https://www.imi.europa.eu/projects-results/project-factsheets/{project_name.lower()}'

            node_dict['Project'][project_name] = Node('Project', **project_property)
            tx.create(node_dict['Project'][project_name])

    # Create pathogen node
    pathogen_df = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'pathogen.csv'),
        dtype=str,
        index_col='id',
    )

    for pathogen_name in pathogen_df.values:
        pathogen_property = {}

        if pd.notna(pathogen_name):
            pathogen_name = pathogen_name[0]
            pathogen_property['name'] = pathogen_name
            node_dict['Pathogen'][pathogen_name] = Node('Pathogen', **pathogen_property)
            tx.create(node_dict['Pathogen'][pathogen_name])

    # Create skill nodes
    skill_df = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'skill.csv'),
        dtype=str,
        index_col='id',
    )

    skill_set_1 = set(skill_df['skill'].tolist())
    skill_set_2 = {
        i + '_group'
        for i in skill_df['category'].unique().tolist()
    }

    skill_set_1 = skill_set_1.union(skill_set_2)

    for skill_name in skill_set_1:
        skill_property = {}

        if pd.notna(skill_name):
            skill_property['name'] = skill_name
            node_dict['Skill'][skill_name] = Node('Skill', **skill_property)
            tx.create(node_dict['Skill'][skill_name])

    return node_dict


def add_relations(tx: Transaction, df: pd.DataFrame, node_mapping_dict: dict):
    """Add relations specific to AMR data. """

    for rows in tqdm(df.values, desc='Populating graph'):
        (
            person_name,
            institute_name,
            project_1_name,
            project_2_name,
            pathogen_1_name,
            pathogen_2_name,
            pathogen_3_name,
            skill_1_name,
            skill_2_name,
            skill_3_name,
            skill_4_name
        ) = rows

        # Person - [WORKS_AT] -> Institute
        person_node = node_mapping_dict['Person'][person_name]
        institute_node = node_mapping_dict['Institute'][institute_name]
        works_at = Relationship(person_node, 'WORKS_AT', institute_node)
        tx.create(works_at)

        # Peron - [IS_INVOLVED_IN] -> Project + Institute -[SUPERVISES] -> Project
        if pd.notna(project_1_name):
            project_1_node = node_mapping_dict['Project'][project_1_name]
            involved_in = Relationship(person_node, 'IS_INVOLVED_IN', project_1_node)
            tx.create(involved_in)

            supervises = Relationship(institute_node, 'SUPERVISES', project_1_node)
            tx.create(supervises)

        if pd.notna(project_2_name) and project_2_name != project_1_name:
            project_2_node = node_mapping_dict['Project'][project_2_name]
            involved_in = Relationship(person_node, 'IS_INVOLVED_IN', project_2_node)
            tx.create(involved_in)

            supervises = Relationship(institute_node, 'SUPERVISES', project_2_node)
            tx.create(supervises)

        # Peron - [HAS_SKILL] -> Skill
        if pd.notna(skill_1_name):
            skill_1_node = node_mapping_dict['Skill'][skill_1_name]
            has_skill = Relationship(person_node, 'HAS_SKILL', skill_1_node)
            tx.create(has_skill)

        if pd.notna(skill_2_name) and skill_2_name != skill_1_name:
            skill_2_node = node_mapping_dict['Skill'][skill_2_name]
            has_skill = Relationship(person_node, 'HAS_SKILL', skill_2_node)
            tx.create(has_skill)

        if pd.notna(skill_3_name) and skill_3_name != skill_2_name and skill_3_name != skill_1_name:
            skill_3_node = node_mapping_dict['Skill'][skill_3_name]
            has_skill = Relationship(person_node, 'HAS_SKILL', skill_3_node)
            tx.create(has_skill)

        # Peron - [WORKS_WITH] -> Pathogen
        if pd.notna(pathogen_1_name):
            pathogen_1_node = node_mapping_dict['Pathogen'][pathogen_1_name]
            works_with = Relationship(person_node, 'WORKS_WITH', pathogen_1_node)
            tx.create(works_with)

        if pd.notna(pathogen_2_name) and pathogen_2_name != pathogen_1_name:
            pathogen_2_node = node_mapping_dict['Pathogen'][pathogen_2_name]
            works_with = Relationship(person_node, 'WORKS_WITH', pathogen_2_node)
            tx.create(works_with)

        if pd.notna(pathogen_3_name) and pathogen_3_name != pathogen_1_name and pathogen_3_name != pathogen_2_name:
            pathogen_3_node = node_mapping_dict['Pathogen'][pathogen_3_name]
            works_with = Relationship(person_node, 'WORKS_WITH', pathogen_3_node)
            tx.create(works_with)


def add_skill_data(tx: Transaction, node_mapping_dict: dict):
    """Add skill category connection to AMR KG. """

    skill_df = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'skill.csv'),
        usecols=[
            'skill',
            'category'
        ]
    )

    skill_df['category'] = skill_df['category'].apply(lambda x: x + '_group')

    for skill_name, skill_class_name in skill_df.values:
        skill_node = node_mapping_dict['Skill'][skill_name]
        skill_class_node = node_mapping_dict['Skill'][skill_class_name]
        includes = Relationship(skill_class_node, 'INCLUDES', skill_node)
        tx.create(includes)


def main(argv):
    db_name = "amr"
    try:
        opts, args = getopt.getopt(argv, "hd:", ["db="])
    except getopt.GetoptError:
        print('amr-script -id <dbname>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('amr-script -d <dbname>')
            sys.exit()
        elif opt in ("-d", "--db"):
            db_name = arg

    tx = populate_db(db_name=db_name)
    df = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'person.csv'),
        usecols=[
            'contact',
            'institute',
            'project_1',
            'project_2',
            'pathogen_1',
            'pathogen_2',
            'pathogen_3',
            'skill_1',
            'skill_2',
            'skill_3',
            'skill_4'
        ]
    )

    df = map_data(data_df=df)

    # Add nodes
    node_map = add_nodes(tx=tx)

    # Add relations
    add_relations(
        tx=tx,
        df=df,
        node_mapping_dict=node_map
    )

    # Add intra-skill relations
    add_skill_data(
        tx=tx,
        node_mapping_dict=node_map
    )
    commit(db_name, tx)


if __name__ == '__main__':
    main(sys.argv[1:])
