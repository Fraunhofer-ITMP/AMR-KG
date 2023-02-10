# -*- coding: utf-8 -*-

"""Script to run the demo expertise knowledge graph"""

import getopt
import os
import sys
import datetime
import logging

import pandas as pd
from py2neo import Node, Relationship
from py2neo.database import Transaction, Graph

from connection import populate_db
from constants import DATA_DIR, ENCODING, ENGINE
from sources import add_chembl, add_spark, add_drug_central
from relations import add_base_data, add_chembl_data, add_spark_data, add_drug_central_data

pd.set_option('display.max_columns', None)
logger = logging.getLogger('__name__')

def map_data(
    data_df: pd.DataFrame
):
    # Map to institute name
    institute_dict = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'institute.csv'),
        dtype=str,
        index_col='id',
        encoding=ENCODING,
        engine=ENGINE
    ).to_dict()['institute']

    data_df['institute'] = data_df['institute'].map(institute_dict)

    # Map to project name
    project_dict = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'project.csv'),
        dtype=str,
        index_col='id',
        encoding=ENCODING,
        engine=ENGINE
    ).to_dict()['project']

    for i in ['project_1', 'project_2']:
        data_df[i] = data_df[i].map(project_dict)

    # Map to bacterial strain
    pathogen_dict = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'pathogen.csv'),
        dtype=str,
        encoding=ENCODING,
        engine=ENGINE
    ).to_dict()['pathogen']

    for i in ['pathogen_1', 'pathogen_2', 'pathogen_3']:
        data_df[i] = data_df[i].map(pathogen_dict)

    # Map to skill set
    skill_dict = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'skill.csv'),
        dtype=str,
        index_col='id',
        encoding=ENCODING,
        engine=ENGINE
    ).to_dict()['skill']

    for i in ['skill_1', 'skill_2', 'skill_3', 'skill_4']:
        data_df[i] = data_df[i].map(skill_dict)

    return data_df


def _update_nodes(
    node_dict: dict,
    tx: Transaction
):
    """Add nodes from dictionary."""
    for node_type in node_dict:
        tx.create(node_dict[node_type])


def add_nodes(tx: Transaction):
    """Add nodes specific to AMR data"""

    node_dict = {
        'Person': {},
        'Institute': {},
        'Skill': {},
        'Pathogen': {},
        'Project': {},
        'ChEMBL': {},
        'SPARK': {},
        'PubChem': {},
        'DrugCentral': {},
    }

    # Create person nodes
    person_df = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'person.csv'),
        usecols=['contact', 'email', 'orcid'],
        encoding=ENCODING,
        engine=ENGINE
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
        usecols=['institute', 'link'],
        encoding=ENCODING,
        engine=ENGINE
    )

    for institute_name, institute_page in institute_df.values:
        institute_property = {}

        if pd.notna(institute_name):
            institute_property['name'] = institute_name
            institute_property['link'] = institute_page

            node_dict['Institute'][institute_name] = Node(
                'Institute', **institute_property
            )
            tx.create(node_dict['Institute'][institute_name])

    # Create project nodes
    project_df = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'project.csv'),
        dtype=str,
        index_col='id',
        encoding=ENCODING,
        engine=ENGINE
    )

    for project_name in project_df.values:
        project_property = {}

        if pd.notna(project_name):
            project_name = project_name[0]
            project_property['name'] = project_name
            project_property['curie'] = 'imi:' + project_name.lower()
            project_property[
                'link'] = f'https://www.imi.europa.eu/projects-results/project-factsheets/{project_name.lower()}'

            node_dict['Project'][project_name] = Node('Project', **project_property)
            tx.create(node_dict['Project'][project_name])

    # Create pathogen node
    pathogen_df = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'pathogen.csv'),
        dtype=str,
        encoding=ENCODING,
        engine=ENGINE
    )

    # List of pathogens from data file
    interested_pathogen = []
    for i in pathogen_df['pathogen'].values:
        if pd.notna(i):
            p = i.split(', ')
            interested_pathogen.extend(p)

    interested_pathogen = set(interested_pathogen)

    for pathogen_name, taxon_id in pathogen_df.values:
        pathogen_property = {}

        if pd.isna(pathogen_name):
            continue

        pathogen_property['name'] = pathogen_name
        pathogen_property['curie'] = 'ncbitaxon:' + taxon_id
        pathogen_property['info'] = f'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id={taxon_id}'
        node_dict['Pathogen'][pathogen_name] = Node('Pathogen', **pathogen_property)
        tx.create(node_dict['Pathogen'][pathogen_name])

    # Create skill nodes
    skill_df = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'skill.csv'),
        dtype=str,
        index_col='id',
        encoding=ENCODING,
        engine=ENGINE
    )

    skill_set_1 = set(skill_df['skill'].tolist())
    skill_set_2 = {i + '_group' for i in skill_df['category'].unique().tolist()}

    skill_set_1 = skill_set_1.union(skill_set_2)

    skill_def = {
        skill: definition
        for skill, definition in skill_df[['skill', 'definition']].values
        if pd.notna(definition)
    }

    for skill_name in skill_set_1:
        skill_property = {}

        if pd.notna(skill_name):
            skill_property['name'] = skill_name
            if skill_name in skill_def:
                skill_property['definition'] = skill_def[skill_name]

            node_dict['Skill'][skill_name] = Node('Skill', **skill_property)
            tx.create(node_dict['Skill'][skill_name])

    """Add ChEMBL data"""

    node_dict, chembl_to_node_map = add_chembl(
        interested_pathogen=interested_pathogen,
        tx=tx,
        node_dict=node_dict
    )

    """Add SPARK data"""

    node_dict = add_spark(
        interested_pathogen=interested_pathogen,
        chembl_to_node_map=chembl_to_node_map,
        node_dict=node_dict
    )

    """Add DrugCentral data"""
    node_dict = add_drug_central(
        node_dict=node_dict,
        chembl_to_node_map=chembl_to_node_map
    )

    # Add also updated nodes into graph
    for node_type in node_dict:
        _update_nodes(
            node_dict=node_dict[node_type],
            tx=tx
        )

    logger.warning(f'Completed node creation!')

    return node_dict


def add_relations(
    tx: Transaction,
    df: pd.DataFrame,
    mic_df: pd.DataFrame,
    spark_df: pd.DataFrame,
    drug_central_df: pd.DataFrame,
    node_mapping_dict: dict
):
    """Add relations specific to AMR data. """

    # Add basic data
    add_base_data(df=df, node_mapping_dict=node_mapping_dict, tx=tx)

    # Add ChEMBL data
    add_chembl_data(df=mic_df, node_mapping_dict=node_mapping_dict, tx=tx)

    # Add spark data
    add_spark_data(df=spark_df, node_mapping_dict=node_mapping_dict, tx=tx)

    # Add Drug Central data
    add_drug_central_data(df=drug_central_df, node_mapping_dict=node_mapping_dict, tx=tx)

    print('#### Node Summary ####')
    for i in node_mapping_dict:
        print(f'{i} - {len(node_mapping_dict[i])}')


def add_skill_data(
    tx: Transaction,
    node_mapping_dict: dict
):
    """Add skill category connection to AMR KG."""

    skill_df = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'skill.csv'),
        usecols=['skill', 'category'],
        encoding=ENCODING,
        engine=ENGINE
    )

    skill_df['category'] = skill_df['category'].apply(lambda x: x + '_group')

    for skill_name, skill_class_name in skill_df.values:
        skill_node = node_mapping_dict['Skill'][skill_name]
        skill_class_node = node_mapping_dict['Skill'][skill_class_name]
        includes = Relationship(skill_class_node, 'INCLUDES', skill_node)
        tx.create(includes)


def add_institute_data(
    tx: Transaction,
    node_mapping_dict: dict
):
    # Map to project name
    project_dict = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'project.csv'),
        dtype=str,
        index_col='id',
        encoding=ENCODING,
        engine=ENGINE
    ).to_dict()['project']

    institute_df = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'institute.csv'),
        usecols=['institute', 'projects'],
        encoding=ENCODING,
        engine=ENGINE
    )

    for row in institute_df.values:
        (
            institute_name,
            projects
        ) = row

        institute_node = node_mapping_dict['Institute'][institute_name]

        for project_idx in projects.split(','):
            if project_idx:
                project_name = project_dict[int(project_idx)]
                project_node = node_mapping_dict['Project'][project_name]

                supervises = Relationship(institute_node, 'SUPERVISES', project_node)
                tx.create(supervises)


def export_triples(
    graph: Graph
):
    """Exporting triples of the graph"""

    DATE = datetime.today().strftime('%d_%b_%Y')
    t = graph.run(
        'Match (n)-[r]-(m) Return n.name, n.curie, type(r), m.name, m.curie'
    ).to_data_frame()

    graph_dir = f'{DATA_DIR}/dump'
    os.makedirs(graph_dir, exist_ok=True)
    return t.to_csv(f'{graph_dir}/base_triples-{DATE}.tsv', sep='\t', index=False)


def main():
    db_name = "amr"
    # try:
    #     opts, args = getopt.getopt(argv, "hd:", ["db="])
    # except getopt.GetoptError:
    #     print("amr -id <dbname>")
    #     sys.exit(2)
    # for opt, arg in opts:
    #     if opt == "-h":
    #         print("amr -d <dbname>")
    #         sys.exit()
    #     elif opt in ("-d", "--db"):
    #         db_name = arg

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
            'skill_4',
        ],
        encoding=ENCODING,
        engine=ENGINE
    )

    df = map_data(data_df=df)

    # Load ChEMBL data
    mic_df = pd.read_csv(
        os.path.join(DATA_DIR, 'MIC', 'data_dump_31.tsv'),
        sep='\t',
        dtype=str,
        encoding=ENCODING,
        engine=ENGINE
    )
    mic_df['mic_val'] = mic_df['standard_value'] + mic_df['standard_units']
    mic_df.drop(['standard_value', 'standard_units'], axis=1, inplace=True)

    # Load SPARK data
    spark_df = pd.read_csv(
        os.path.join(DATA_DIR, 'SPARK', 'processed_mic_data.tsv'),
        sep='\t',
        dtype=str,
        encoding=ENCODING,
        engine=ENGINE
    )
    spark_df.drop_duplicates(inplace=True)

    drug_central_df = pd.read_csv(
        os.path.join(DATA_DIR, 'drug_central', 'drug.target.interaction.tsv'),
        sep='\t',
        dtype=str,
        usecols=[
            'STRUCT_ID',
            'ACT_VALUE',
            'ACT_UNIT',
            'ACT_TYPE',
            'ACT_SOURCE_URL',
            'ORGANISM',
        ]
    )
    drug_central_df = drug_central_df.astype({'STRUCT_ID': 'str'})

    dc_chembl_mapper = pd.read_csv(
        os.path.join(DATA_DIR, 'drug_central', 'drug_target_harmonized.tsv'),
        sep='\t',
        dtype=str,
        usecols=['STRUCT_ID', 'chembl_id'],
        index_col='STRUCT_ID'
    ).to_dict()['chembl_id']

    drug_central_df['chembl_id'] = drug_central_df['STRUCT_ID'].map(dc_chembl_mapper)
    drug_central_df.drop_duplicates(inplace=True)

    # Add nodes
    node_map = add_nodes(tx=tx)

    # Add relations
    add_relations(
        tx=tx,
        df=df,
        mic_df=mic_df,
        spark_df=spark_df,
        drug_central_df=drug_central_df,
        node_mapping_dict=node_map
    )

    # Add intra-skill relations
    add_skill_data(tx=tx, node_mapping_dict=node_map)

    # Add institute-project edges
    add_institute_data(tx=tx, node_mapping_dict=node_map)
    tx.commit()


if __name__ == "__main__":
    # main(sys.argv[1:])
    main()

