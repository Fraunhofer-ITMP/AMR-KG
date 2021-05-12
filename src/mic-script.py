# -*- coding: utf-8 -*-

import os
import pandas as pd
from tqdm import tqdm

from py2neo import Node, Relationship
from py2neo.database.work import Transaction

from constants import DATA_DIR
from connection import populate_db


def create_nodes(tx: Transaction, data: pd.DataFrame):
    """Create nodes specific for MIC"""

    node_map = {
        'Bacteria': {},
        'Chemical': {},
        'IC50': {},
        'Journal': {},
        'Year': {}
    }

    # Create bacterial strains
    bacterial_strain = data['strain'].unique().tolist()
    for bacteria_name in bacterial_strain:
        bacterial_property = {}

        if pd.notna(bacteria_name):
            bacterial_property['name'] = bacteria_name

        node_map['Bacteria'][bacteria_name] = Node(
            'Bacteria',
            **bacterial_property
        )
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
        tx.create(node_map['IC50'][val])

    # Add journal information
    journal_names = data['Document Journal'].unique().tolist()
    journal_names = ['Assay test' if pd.isna(x) else x for x in journal_names]

    for name in journal_names:
        journal_property = {}

        if pd.notna(name):
            journal_property['name'] = name

        node_map['Journal'][name] = Node('Journal', **journal_property)
        tx.create(node_map['Journal'][name])

    # Add journal year
    publication_years = data['Document Year'].unique().tolist()

    for year in publication_years:
        year_property = {}

        if pd.notna(year):
            year_property['year'] = year

            node_map['Year'][year] = Node('Year', **year_property)
            tx.create(node_map['Year'][year])

    return node_map


def create_relations(
        df: pd.DataFrame,
        node_map: dict,
        tx: Transaction
):
    """Add relations specific to MIC"""

    for row in tqdm(df.values, desc='Adding relations'):
        (
            strain,
            chembl_id,  # not needed
            chemical,
            pval,
            assay_id,
            doc_name,
            doc_year
        ) = row

        bact_node = node_map['Bacteria'][strain]
        chem_node = node_map['Chemical'][chemical]

        assay_property = {}
        if pd.notna(assay_id):
            assay_property['assay_info'] = f'https://www.ebi.ac.uk/chembl/assay_report_card/{assay_id}/'

            assay_in = Relationship(
                bact_node,
                'ASSAY_IN',
                chem_node,
                **assay_property
            )
            tx.create(assay_in)

        if pd.notna(pval):
            p_node = node_map['IC50'][pval]

            has_pic50 = Relationship(chem_node, 'HAS_pIC50', p_node)
            tx.create(has_pic50)

        if pd.notna(doc_name):
            journal_node = node_map['Journal'][doc_name]

            found_in = Relationship(chem_node, 'FOUND_IN', journal_node)
            tx.create(found_in)

        if pd.notna(doc_year):
            year_node = node_map['Year'][doc_year]

            in_year = Relationship(chem_node, 'IN_YEAR', year_node)
            tx.create(in_year)


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

    node_mapping_dict = create_nodes(
        tx=tx,
        data=data_df
    )

    data_df['Document Journal'] = data_df['Document Journal'].fillna(value='Assay test')

    create_relations(
        df=data_df,
        tx=tx,
        node_map=node_mapping_dict
    )

    tx.commit()


if __name__ == '__main__':
    main()
