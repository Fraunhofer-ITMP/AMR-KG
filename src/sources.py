# -*- coding: utf-8 -*-

"""Script for ingestion of new data sources"""

import os
import pandas as pd
from tqdm import tqdm
from pubchempy import Compound, get_compounds
from py2neo import Node
from constants import DATA_DIR, ENCODING, PATHOGEN_MAPPER


def add_chembl(interested_pathogen, node_dict, tx):
    """Add ChEMBL data"""

    mic_df = pd.read_csv(
        os.path.join(DATA_DIR, 'MIC', 'data_dump_31.tsv'),
        sep='\t',
        dtype=str,
        usecols=[
            'strain',
            'pref_name',
            'chembl_id',
        ],
        encoding=ENCODING,
    )

    mic_df = mic_df.loc[mic_df['strain'].isin(interested_pathogen)]
    mic_df.drop('strain', axis=1, inplace=True)
    mic_df.drop_duplicates(inplace=True)

    chembl_to_node_map = {}

    # Create chemical nodes
    for name, chembl_id in tqdm(mic_df.values):
        chemical_property = {}

        if pd.notna(chembl_id):
            chemical_property['curie'] = 'chembl:' + chembl_id
            chemical_property['info'] = f'https://www.ebi.ac.uk/chembl/compound_report_card/{chembl_id}/'
            chembl_to_node_map[chembl_id] = name.title() if pd.notna(name) else '' # To merge duplicates in chembl

        if pd.notna(name):
            chemical_property['name'] = name.title()

        node_dict['ChEMBL'][name] = Node('ChEMBL', **chemical_property)
        tx.create(node_dict['ChEMBL'][name])

    return node_dict, chembl_to_node_map


def add_spark(
    interested_pathogen: set,
    node_dict: dict,
    chembl_to_node_map: dict
):
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
        ]
    )
    spark_df = spark_df[spark_df['Curated & Transformed MIC Data: Species'].isin(interested_pathogen)]
    spark_df.drop('Curated & Transformed MIC Data: Species', axis=1, inplace=True)

    spark_df.drop_duplicates(inplace=True)

    if spark_df.empty:
        return node_dict

    for spark_id, smiles, pubchem_id, chembl_id in tqdm(
        spark_df.values, desc='Getting information from SPARK data'
    ):
        chemical_property = {}

        if pd.notna(smiles):
            chemical_property['SMILES'] = smiles

        name = chembl_id  # Set default

        if pd.notna(pubchem_id):  # id stored in float format
            pubchem_id = pubchem_id.split('.')[0]

        if pd.isna(chembl_id) and pd.isna(pubchem_id):
            if spark_id in node_dict['SPARK']:
                continue

            if pd.notna(spark_id):
                chemical_property['curie'] = 'spark:' + spark_id

            node_dict['SPARK'][spark_id] = Node('SPARK', **chemical_property)

        elif pd.notna(chembl_id):  # If chembl id exists (higher priority)
            chemical_property['Spark ID'] = 'spark:' + spark_id

            if pd.notna(pubchem_id):
                chemical_property['PubChem ID'] = 'pubchem:' + pubchem_id
                chemical_property['info'] = f'https://pubchem.ncbi.nlm.nih.gov/compound/{pubchem_id}'
                name = Compound.from_cid(pubchem_id).synonyms[0]

            if chembl_id in chembl_to_node_map:
                chembl_node_name = chembl_to_node_map[chembl_id]  # Get name of node
                node_dict['ChEMBL'][chembl_node_name].update(chemical_property)
            else:
                chemical_property['curie'] = 'chembl' + chembl_id
                chemical_property['info'] = f'https://www.ebi.ac.uk/chembl/compound_report_card/{chembl_id}/'
                chemical_property['name'] = name
                node_dict['ChEMBL'][name] = Node('ChEMBL', **chemical_property)
        else:  # If pubchem id exists
            chemical_property['Spark ID'] = 'spark:' + spark_id
            name = Compound.from_cid(pubchem_id).iupac_name

            if pubchem_id in node_dict['PubChem']:
                continue

            chemical_property['name'] = name
            chemical_property['curie'] = 'pubchem:' + pubchem_id
            chemical_property['info'] = f'https://pubchem.ncbi.nlm.nih.gov/compound/{pubchem_id}'
            node_dict['PubChem'][pubchem_id] = Node('PubChem', **chemical_property)

    return node_dict


def add_drug_central(
    node_dict: dict,
):
    drug_central_df = pd.read_csv(
        os.path.join(DATA_DIR, 'drug_central', 'drug.target.interaction.tsv'),
        sep='\t',
        dtype=str,
        usecols=[
            'DRUG_NAME',
            'STRUCT_ID',
            'ACT_VALUE',
            'ACT_UNIT',
            'ACT_TYPE',
            'ACT_SOURCE_URL',
            'ORGANISM'
        ]
    )

    drug_central_df = drug_central_df[drug_central_df['ORGANISM'].isin(PATHOGEN_MAPPER)]
    drug_central_df.drop('ORGANISM', axis=1, inplace=True)
    drug_central_df = drug_central_df[['DRUG_NAME', 'STRUCT_ID']]
    drug_central_df.drop_duplicates(inplace=True)

    if drug_central_df.empty:
        return node_dict

    drug_central_df.to_csv(
        os.path.join(DATA_DIR, 'drug_central', 'drug_target_filtered.tsv'),
        sep='\t',
        index=False
    )

    for drug_name, drug_central_id in tqdm(drug_central_df.values, desc='Getting information from DrugCentral'):
        chemical_property = {}

        try:
            pubchem_ids = get_compounds(identifier=drug_name, namespace='name')
        except Exception:
            pubchem_ids = []

        if len(pubchem_ids) > 0:
            pubchem_id = pubchem_ids[0].cid
            chemical_property['curie'] = 'pubchem:' + str(pubchem_id)
            chemical_property['info'] = f'https://pubchem.ncbi.nlm.nih.gov/compound/{pubchem_id}'
            chemical_property['DrugCentral ID'] = 'drug.central:' + drug_central_id
            name = Compound.from_cid(pubchem_id).synonyms[0]
            chemical_property['name'] = name
            node_dict['PubChem'][drug_central_id] = Node('PubChem', **chemical_property)
        else:
            chemical_property['curie'] = 'drug.central:' + drug_central_id
            chemical_property['info'] = f'https://drugcentral.org/drugcard/{drug_central_id}'
            chemical_property['name'] = drug_name
            node_dict['DrugCentral'][drug_central_id] = Node('DrugCentral', **chemical_property)

    return node_dict
