# -*- coding: utf-8 -*-

import os
import pandas as pd

from py2neo import Node, Relationship
from py2neo.database.work import Transaction

from constants import DATA_DIR
from connection import populate_db


def create_nodes(tx: Transaction, data: pd.DataFrame):
    """Create nodes specific for MIC"""

    node_map = {
        'Bacteria': {},
        'Chemical': {},
        'IC50': {}
    }

    # Create bacterial strains
    bacterial_strain = data['strain'].unique().tolist()
    for bacteria_name in bacterial_strain:
        bacterial_property = {}

        if pd.notna(bacteria_name):
            bacterial_property['name'] = bacteria_name

        node_map['Bacteria'][bacteria_name] = Node('Bacteria', **bacterial_property)
        tx.create(node_map['Bacteria'][bacteria_name])

    # Create chemical nodes
    chemical_data = data[['Molecule ChEMBL ID', 'NAME']]
    chemical_data = chemical_data.drop_duplicates()
    for chembl_id, name in chemical_data.values:
        chemical_property = {}

        if pd.notna(chembl_id):
            chemical_property['chembl'] = f'https://www.ebi.ac.uk/chembl/compound_report_card/{chembl_id}/'

        if pd.notna(name):
            chemical_property['name'] = name

        node_map['Chemical'][name] = Node('Chemical', **chemical_property)
        tx.create(node_map['Chemical'][name])

    # Create pIC50 nodes
    ic50 = data['pIC50'].unique().tolist()
    for val in ic50:
        ic50_property = {}

        if pd.notna(val):
            ic50_property['name'] = val
        node_map['IC50'][val] = Node('IC50', **ic50_property)

    return node_map


def create_relations():
    return None


def main():
    tx = populate_db(db_name='micdata')

    data_df = pd.read_csv(
        os.path.join(DATA_DIR, 'MIC', 'mic-data.tsv'),
        sep='\t',
        dtype=str,
        usecols=[
            'strain',
            'Molecule ChEMBL ID',
            'NAME',
            'pIC50',
            'Assay ChEMBL ID',
            'Document Year',
            'Document Journal'
        ]
    )
    # pd.set_option('display.max_columns', None)
    # print(data_df)

    node_mapping_dict = create_nodes(tx=tx, data=data_df)

    for strain, chemical, assay_id in data_df[['strain', 'NAME', 'Assay ChEMBL ID']].values:
        bact_node = node_mapping_dict['Bacteria'][strain]
        chem_node = node_mapping_dict['Chemical'][chemical]

        assay_property = {}
        if pd.notna(assay_id):
            assay_property['assay_info'] = f'https://www.ebi.ac.uk/chembl/assay_report_card/{assay_id}/'

        assay_in = Relationship(bact_node, 'ASSAY_IN', chem_node, **assay_property)
        tx.create(assay_in)

    tx.commit()

if __name__ == '__main__':
    main()
