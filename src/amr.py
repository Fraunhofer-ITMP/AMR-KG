# -*- coding: utf-8 -*-

"""Script to run the demo expertise knowledge graph"""

import getopt
import os
import sys

import pandas as pd
from pubchempy import Compound
from py2neo import Node, Relationship
from py2neo.database import Transaction
from tqdm import tqdm

from connection import populate_db
from constants import DATA_DIR
from constants import ENCODING
from constants import ENGINE


def map_data(
    data_df: pd.DataFrame
):
    # Map to institute name
    institute_dict = pd.read_csv(
        os.path.join(DATA_DIR, "AMR", "institute.csv"),
        dtype=str,
        index_col="id",
        encoding=ENCODING,
        engine=ENGINE
    ).to_dict()["institute"]

    data_df["institute"] = data_df["institute"].map(institute_dict)

    # Map to project name
    project_dict = pd.read_csv(
        os.path.join(DATA_DIR, "AMR", "project.csv"),
        dtype=str,
        index_col="id",
        encoding=ENCODING,
        engine=ENGINE
    ).to_dict()["project"]

    for i in ["project_1", "project_2"]:
        data_df[i] = data_df[i].map(project_dict)

    # Map to bacterial strain
    pathogen_dict = pd.read_csv(
        os.path.join(DATA_DIR, "AMR", "pathogen.csv"),
        dtype=str,
        encoding=ENCODING,
        engine=ENGINE
    ).to_dict()["pathogen"]

    for i in ["pathogen_1", "pathogen_2", "pathogen_3"]:
        data_df[i] = data_df[i].map(pathogen_dict)

    # Map to skill set
    skill_dict = pd.read_csv(
        os.path.join(DATA_DIR, "AMR", "skill.csv"),
        dtype=str,
        index_col="id",
        encoding=ENCODING,
        engine=ENGINE
    ).to_dict()["skill"]

    for i in ["skill_1", "skill_2", "skill_3", "skill_4"]:
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
    }

    # Create person nodes
    person_df = pd.read_csv(
        os.path.join(DATA_DIR, "AMR", "person.csv"),
        usecols=["contact", "email", "orcid"],
        encoding=ENCODING,
        engine=ENGINE
    )

    for name, email, orcid in person_df.values:
        person_property = {}

        if pd.notna(name):
            person_property["name"] = name

        if pd.notna(email):
            person_property["email"] = email

        if pd.notna(orcid):
            person_property["orcid"] = orcid

        node_dict["Person"][name] = Node("Person", **person_property)
        tx.create(node_dict["Person"][name])

    # Create institute nodes
    institute_df = pd.read_csv(
        os.path.join(DATA_DIR, "AMR", "institute.csv"),
        usecols=['institute', 'link'],
        encoding=ENCODING,
        engine=ENGINE
    )

    for institute_name, institute_page in institute_df.values:
        institute_property = {}

        if pd.notna(institute_name):
            institute_property["name"] = institute_name
            institute_property["link"] = institute_page

            node_dict["Institute"][institute_name] = Node(
                "Institute", **institute_property
            )
            tx.create(node_dict["Institute"][institute_name])

    # Create project nodes
    project_df = pd.read_csv(
        os.path.join(DATA_DIR, "AMR", "project.csv"),
        dtype=str,
        index_col="id",
        encoding=ENCODING,
        engine=ENGINE
    )

    for project_name in project_df.values:
        project_property = {}

        if pd.notna(project_name):
            project_name = project_name[0]
            project_property["name"] = project_name
            project_property[
                "link"] = f"https://www.imi.europa.eu/projects-results/project-factsheets/{project_name.lower()}"

            node_dict["Project"][project_name] = Node("Project", **project_property)
            tx.create(node_dict["Project"][project_name])

    # Create pathogen node
    pathogen_df = pd.read_csv(
        os.path.join(DATA_DIR, "AMR", "pathogen.csv"),
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
        pathogen_property['ncbi id'] = taxon_id
        pathogen_property['info'] = f'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id={taxon_id}'
        node_dict['Pathogen'][pathogen_name] = Node('Pathogen', **pathogen_property)
        tx.create(node_dict['Pathogen'][pathogen_name])

    # Create skill nodes
    skill_df = pd.read_csv(
        os.path.join(DATA_DIR, "AMR", "skill.csv"),
        dtype=str,
        index_col="id",
        encoding=ENCODING,
        engine=ENGINE
    )

    skill_set_1 = set(skill_df["skill"].tolist())
    skill_set_2 = {i + "_group" for i in skill_df["category"].unique().tolist()}

    skill_set_1 = skill_set_1.union(skill_set_2)

    skill_def = {
        skill: definition
        for skill, definition in skill_df[['skill', 'definition']].values
        if pd.notna(definition)
    }

    for skill_name in skill_set_1:
        skill_property = {}

        if pd.notna(skill_name):
            skill_property["name"] = skill_name
            if skill_name in skill_def:
                skill_property['definition'] = skill_def[skill_name]

            node_dict["Skill"][skill_name] = Node("Skill", **skill_property)
            tx.create(node_dict["Skill"][skill_name])

    """Add ChEMBL data"""

    mic_df = pd.read_csv(
        os.path.join(DATA_DIR, 'MIC', 'mic-data.tsv'),
        sep='\t',
        dtype=str,
        usecols=[
            'strain',
            'Molecule ChEMBL ID',
            'NAME',
        ],
        encoding=ENCODING,
        engine=ENGINE
    )

    mic_df = mic_df.loc[mic_df['strain'].isin(interested_pathogen)]
    mic_df.drop('strain', axis=1, inplace=True)
    mic_df.drop_duplicates(inplace=True)

    chembl_to_node_map = {}

    # Create chemical nodes
    if not mic_df.empty:
        for chembl_id, name in mic_df.values:
            chemical_property = {}

            if pd.notna(chembl_id):
                chemical_property['ChEMBL ID'] = chembl_id
                chemical_property['info'] = f'https://www.ebi.ac.uk/chembl/compound_report_card/{chembl_id}/'
                chembl_to_node_map[chembl_id] = name  # To merge duplicates in chembl

            if pd.notna(name):
                chemical_property['name'] = name

            node_dict['ChEMBL'][name] = Node('ChEMBL', **chemical_property)
            tx.create(node_dict['ChEMBL'][name])

    """ADD SPARK DATA """

    spark_df = pd.read_csv(
        os.path.join(DATA_DIR, 'SPARK', 'processed_mic_data.tsv'),
        sep='\t',
        dtype=str,
        usecols=[
            'Compound Name',
            'SMILES',
            'Curated & Transformed MIC Data: Species',
            'pubchem',
            'chembl'
        ],
        encoding=ENCODING,
        engine=ENGINE
    )
    spark_df = spark_df[spark_df['Curated & Transformed MIC Data: Species'].isin(interested_pathogen)]
    spark_df.drop('Curated & Transformed MIC Data: Species', axis=1, inplace=True)

    spark_df.drop_duplicates(inplace=True)

    if not spark_df.empty:
        for spark_id, smiles, pubchem_id, chembl_id in tqdm(spark_df.values):
            chemical_property = {}

            name = chembl_id  # Set default

            if pd.notna(pubchem_id):
                pubchem_id = pubchem_id.split('.')[0]

            if pd.isna(chembl_id) and pd.isna(pubchem_id):
                if spark_id in node_dict['SPARK']:
                    continue

                chemical_property.update({
                    'SPARK ID': spark_id,
                    'SMILES': smiles
                })
                node_dict['SPARK'][spark_id] = Node('SPARK', **chemical_property)
            elif pd.notna(chembl_id):  # If chembl id exists
                if pd.notna(spark_id):
                    chemical_property['Spark ID'] = spark_id

                if pd.notna(pubchem_id):
                    chemical_property['PubChem ID'] = pubchem_id
                    chemical_property['info'] = f'https://pubchem.ncbi.nlm.nih.gov/compound/{pubchem_id}'
                    name = Compound.from_cid(pubchem_id).synonyms[0]

                if chembl_id in chembl_to_node_map:
                    chembl_node_name = chembl_to_node_map[chembl_id]  # Get name of node
                    node_dict['ChEMBL'][chembl_node_name].update(chemical_property)
                else:
                    chemical_property['ChEMBL ID'] = chembl_id
                    chemical_property['info'] = f'https://www.ebi.ac.uk/chembl/compound_report_card/{chembl_id}/'
                    chemical_property['Name'] = name
                    node_dict['ChEMBL'][name] = Node('ChEMBL', **chemical_property)
            else:  # If pubchem id exists
                name = Compound.from_cid(pubchem_id).iupac_name

                if pubchem_id in node_dict['PubChem']:
                    continue

                chemical_property['Name'] = name
                chemical_property['PubChem ID'] = pubchem_id
                chemical_property['info'] = f'https://pubchem.ncbi.nlm.nih.gov/compound/{pubchem_id}'
                chemical_property['Spark ID'] = spark_id
                node_dict['PubChem'][pubchem_id] = Node('PubChem', **chemical_property)

    # Add also updated nodes into graph
    for node_type in node_dict:
        _update_nodes(
            node_dict=node_dict[node_type],
            tx=tx
        )

    return node_dict


def add_relations(
    tx: Transaction,
    df: pd.DataFrame,
    mic_df: pd.DataFrame,
    spark_df: pd.DataFrame,
    node_mapping_dict: dict
):
    """Add relations specific to AMR data. """

    for rows in tqdm(df.values, desc="Populating graph"):
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
            skill_4_name,
        ) = rows

        # Person - [WORKS_AT] -> Institute
        person_node = node_mapping_dict["Person"][person_name]
        institute_node = node_mapping_dict["Institute"][institute_name]
        works_at = Relationship(person_node, "WORKS_AT", institute_node)
        tx.create(works_at)

        # Peron - [IS_INVOLVED_IN] -> Project + Institute -[SUPERVISES] -> Project
        if pd.notna(project_1_name):
            project_1_node = node_mapping_dict["Project"][project_1_name]
            involved_in = Relationship(person_node, "IS_INVOLVED_IN", project_1_node)
            tx.create(involved_in)

            supervises = Relationship(institute_node, "SUPERVISES", project_1_node)
            tx.create(supervises)

        if pd.notna(project_2_name) and project_2_name != project_1_name:
            project_2_node = node_mapping_dict["Project"][project_2_name]
            involved_in = Relationship(person_node, "IS_INVOLVED_IN", project_2_node)
            tx.create(involved_in)

        # Peron - [HAS_SKILL] -> Skill
        if pd.notna(skill_1_name):
            skill_1_node = node_mapping_dict["Skill"][skill_1_name]
            has_skill = Relationship(person_node, "HAS_SKILL", skill_1_node)
            tx.create(has_skill)

        if pd.notna(skill_2_name) and skill_2_name != skill_1_name:
            skill_2_node = node_mapping_dict["Skill"][skill_2_name]
            has_skill = Relationship(person_node, "HAS_SKILL", skill_2_node)
            tx.create(has_skill)

        if (
            pd.notna(skill_3_name)
            and skill_3_name != skill_2_name
            and skill_3_name != skill_1_name
        ):
            skill_3_node = node_mapping_dict["Skill"][skill_3_name]
            has_skill = Relationship(person_node, "HAS_SKILL", skill_3_node)
            tx.create(has_skill)

        # Peron - [WORKS_WITH] -> Pathogen
        if pd.notna(pathogen_1_name):
            pathogen_1_node = node_mapping_dict["Pathogen"][pathogen_1_name]
            works_with = Relationship(person_node, "WORKS_WITH", pathogen_1_node)
            tx.create(works_with)

        if pd.notna(pathogen_2_name) and pathogen_2_name != pathogen_1_name:
            pathogen_2_node = node_mapping_dict["Pathogen"][pathogen_2_name]
            works_with = Relationship(person_node, "WORKS_WITH", pathogen_2_node)
            tx.create(works_with)

        if (
            pd.notna(pathogen_3_name)
            and pathogen_3_name != pathogen_1_name
            and pathogen_3_name != pathogen_2_name
        ):
            pathogen_3_node = node_mapping_dict["Pathogen"][pathogen_3_name]
            works_with = Relationship(person_node, "WORKS_WITH", pathogen_3_node)
            tx.create(works_with)

    for row in tqdm(mic_df.values, desc='Adding MIC relations'):
        (
            strain,
            chembl_id,  # not needed
            chemical,
            assay_id,
            mic_val
        ) = row

        if strain not in node_mapping_dict['Pathogen']:  # Omitted as no one works with that strain
            continue

        bact_node = node_mapping_dict['Pathogen'][strain]
        chem_node = node_mapping_dict['ChEMBL'][chemical]

        assay_property = {}
        if pd.notna(assay_id):
            assay_property['ChEMBL Assay'] = f'https://www.ebi.ac.uk/chembl/assay_report_card/{assay_id}/'

        if pd.notna(mic_val):
            assay_property['MIC'] = mic_val

        assay_in = Relationship(
            bact_node,
            'ASSAY IN',
            chem_node,
            **assay_property
        )
        tx.create(assay_in)

    for row in tqdm(spark_df.values, desc='Adding SPARK relations'):
        (
            spark_id,
            smiles,
            pubmed_id,
            mic_val,
            specie,
            doi,
            pubchem_id,
            chembl_id,
        ) = row

        if specie not in node_mapping_dict['Pathogen']:  # Omitted as no one works with that strain
            continue

        bact_node = node_mapping_dict['Pathogen'][specie]
        try:
            chem_node = node_mapping_dict['SPARK'][spark_id]
        except KeyError:
            try:
                if pd.notna(chembl_id):
                    chem_node = node_mapping_dict['ChEMBL'][chembl_id]
                else:
                    chem_node = node_mapping_dict['PubChem'][pubchem_id.split('.')[0]]
            except KeyError:
                continue

        assay_property = {}

        if pd.notna(mic_val):
            assay_property['MIC'] = f'{mic_val} microM'

        if pd.notna(pubmed_id):
            assay_property['Literature'] = f'https://pubmed.ncbi.nlm.nih.gov/{pubmed_id}/'

        if pd.notna(doi):
            assay_property['DOI'] = doi

        assay_in = Relationship(
            bact_node,
            'ASSAY IN',
            chem_node,
            **assay_property
        )
        tx.create(assay_in)


def add_skill_data(
    tx: Transaction,
    node_mapping_dict: dict
):
    """Add skill category connection to AMR KG."""

    skill_df = pd.read_csv(
        os.path.join(DATA_DIR, "AMR", "skill.csv"),
        usecols=["skill", "category"],
        encoding=ENCODING,
        engine=ENGINE
    )

    skill_df["category"] = skill_df["category"].apply(lambda x: x + "_group")

    for skill_name, skill_class_name in skill_df.values:
        skill_node = node_mapping_dict["Skill"][skill_name]
        skill_class_node = node_mapping_dict["Skill"][skill_class_name]
        includes = Relationship(skill_class_node, "INCLUDES", skill_node)
        tx.create(includes)


def add_institute_data(
    tx: Transaction,
    node_mapping_dict: dict
):
    # Map to project name
    project_dict = pd.read_csv(
        os.path.join(DATA_DIR, "AMR", "project.csv"),
        dtype=str,
        index_col="id",
        encoding=ENCODING,
        engine=ENGINE
    ).to_dict()["project"]

    institute_df = pd.read_csv(
        os.path.join(DATA_DIR, "AMR", "institute.csv"),
        usecols=['institute', 'projects'],
        encoding=ENCODING,
        engine=ENGINE
    )

    for row in institute_df.values:
        (
            institute_name,
            projects
        ) = row

        institute_node = node_mapping_dict["Institute"][institute_name]

        for project_idx in projects.split(","):
            if project_idx:
                project_name = project_dict[int(project_idx)]
                project_node = node_mapping_dict['Project'][project_name]

                supervises = Relationship(institute_node, "SUPERVISES", project_node)
                tx.create(supervises)


def main(argv):
    db_name = "amr"
    try:
        opts, args = getopt.getopt(argv, "hd:", ["db="])
    except getopt.GetoptError:
        print("amr -id <dbname>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-h":
            print("amr -d <dbname>")
            sys.exit()
        elif opt in ("-d", "--db"):
            db_name = arg

    tx = populate_db(db_name=db_name)
    df = pd.read_csv(
        os.path.join(DATA_DIR, "AMR", "person.csv"),
        usecols=[
            "contact",
            "institute",
            "project_1",
            "project_2",
            "pathogen_1",
            "pathogen_2",
            "pathogen_3",
            "skill_1",
            "skill_2",
            "skill_3",
            "skill_4",
        ],
        encoding=ENCODING,
        engine=ENGINE
    )

    df = map_data(data_df=df)

    # Load ChEMBL data
    mic_df = pd.read_csv(
        os.path.join(DATA_DIR, 'MIC', 'mic-data.tsv'),
        sep='\t',
        dtype=str,
        usecols=[
            'strain',
            'Molecule ChEMBL ID',
            'NAME',
            'Standard Value',
            'Standard Units',
            'Assay ChEMBL ID',
        ]
    )
    mic_df['mic_val'] = mic_df['Standard Value'] + ' ' + mic_df['Standard Units']
    mic_df.drop(['Standard Value', 'Standard Units'], axis=1, inplace=True)

    # Load SPARK data
    spark_df = pd.read_csv(
        os.path.join(DATA_DIR, 'SPARK', 'processed_mic_data.tsv'),
        sep='\t',
        dtype=str
    )
    spark_df.drop_duplicates(inplace=True)

    # Add nodes
    node_map = add_nodes(tx=tx)

    # Add relations
    add_relations(
        tx=tx,
        df=df,
        mic_df=mic_df,
        spark_df=spark_df,
        node_mapping_dict=node_map
    )

    # Add intra-skill relations
    add_skill_data(tx=tx, node_mapping_dict=node_map)

    # Add institute-project edges
    add_institute_data(tx=tx, node_mapping_dict=node_map)
    tx.commit()


if __name__ == "__main__":
    main(sys.argv[1:])
