# -*- coding: utf-8 -*-

"""Script for ingestion of relations from new sources"""

import pandas as pd
from tqdm import tqdm

from py2neo import Relationship
from constants import PATHOGEN_MAPPER


def add_base_data(df: pd.DataFrame, node_mapping_dict: dict, tx):
    """Add basic member related information"""

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


def add_chembl_data(df: pd.DataFrame, node_mapping_dict: dict, tx):
    """Add ChEMBL Data"""
    for row in tqdm(df.values, desc='Adding MIC relations'):
        (
            compound_name,
            chembl_id,  # not needed
            assay_rel,
            assay_type,
            strain,
            assay_id,
            mic_val
        ) = row

        if strain not in node_mapping_dict['Pathogen']:  # Omitted as no one works with that strain
            continue

        bact_node = node_mapping_dict['Pathogen'][strain]
        chem_node = node_mapping_dict['ChEMBL'][compound_name]

        assay_property = {}
        if pd.notna(assay_id):
            assay_property['ChEMBL Assay'] = f'https://www.ebi.ac.uk/chembl/assay_report_card/{assay_id}/'

        if pd.isna(mic_val) and pd.isna(assay_rel):
            continue

        assay_property['MIC'] = str(assay_rel) + str(mic_val)

        assay_in = Relationship(
            bact_node,
            'ASSAY IN',
            chem_node,
            **assay_property
        )
        tx.create(assay_in)


def add_spark_data(df: pd.DataFrame, node_mapping_dict: dict, tx):
    """Add SPARK IC50 Data"""
    for row in tqdm(df.values, desc='Adding SPARK relations'):
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


def add_drug_central_data(df: pd.DataFrame, node_mapping_dict: dict, tx):
    """Add DrugCentral Data"""

    # Map bacteria names to those in KG
    df['ORGANISM'] = df['ORGANISM'].map(lambda x: PATHOGEN_MAPPER.get(x))

    for row in tqdm(df.values, desc='Adding DrugCentral data'):
        (
            drug_id,
            activity_value,
            activity_unit,
            activity_type,
            source,
            pathogen
        ) = row

        if pathogen not in node_mapping_dict['Pathogen']:  # Omitted as no one works with that strain
            continue

        bact_node = node_mapping_dict['Pathogen'][pathogen]

        try:
            chem_node = node_mapping_dict['PubChem'][drug_id]
        except KeyError:
            try:
                chem_node = node_mapping_dict['DrugCentral'][drug_id]
            except KeyError:
                continue

        assay_property = {}

        if pd.notna(activity_type):
            assay_property[f'{activity_type}'] = f'{activity_value} + {activity_unit}'

        if pd.notna(source):
            assay_property['Literature'] = source

        assay_in = Relationship(
            bact_node,
            'ASSAY IN',
            chem_node,
            **assay_property
        )
        tx.create(assay_in)


