# -*- coding: utf-8 -*-

import os
import pandas as pd

from py2neo import Node, Relationship

from constants import DATA_DIR
from connection import populate_db


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


def add_nodes(tx):

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

    for institute_name in institute_df.values:
        institute_property = {}

        if pd.notna(institute_name):
            institute_name = institute_name[0]
            institute_property['name'] = institute_name

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

    skill_list_1 = set(skill_df['skill'].tolist())
    skill_list_2 = set(skill_df['category'].unique().tolist())

    skill_list_1.union(skill_list_2)

    for skill_name in skill_list_1:
        skill_property = {}

        if pd.notna(skill_name):
            skill_property['name'] = skill_name
            node_dict['Skill'][skill_name] = Node('Skill', **skill_property)
            tx.create(node_dict['Skill'][skill_name])

    return node_dict


def main():
    tx = populate_db(db_name='amrtest')

    df = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'person.csv'),
    )

    df = map_data(data_df=df)

    node_map = add_nodes(tx=tx)

    # pd.set_option('display.max_columns', None)
    # print(df)

    # for name, email in data_df[['Contact', 'email']].values:
    # person_node = Node('Person', name=name)
    #     tx.create(person_node)
    #
    #     email_node = Node('Data', email="mailto:john@example.com")
    #     tx.create(email_node)
    #
    #     orcid_node = Node('ORCID', orcid='https://orcid.org/0000-0002-7683-0452')  # split node to view only number
    #     tx.create(orcid_node)
    #
    #     person_attr_1 = Relationship(person_node, 'has_email', email_node)
    #     perosn_attr_2 = Relationship(person_node, 'has_orcid', orcid_node)
    #     tx.create(person_attr_1)
    #     tx.create(perosn_attr_2)

    tx.commit()


if __name__ == '__main__':
    main()
