# -*- coding: utf-8 -*-

"""Source code to get latest data from ChEMBL."""

import chembl_downloader
import pandas as pd

pd.set_option('display.max_columns', None)


def get_data():
    """Method to get AMR related data from ChEMBL."""

    bacterial_species = pd.read_csv(f'../data/AMR/pathogen.csv')['pathogen'].to_list()

    version, path = chembl_downloader.download_extract_sqlite(return_version=True)

    get_assays = """
    SELECT
            MOLECULE_DICTIONARY.pref_name,
            MOLECULE_DICTIONARY.molregno as chembl_id,
            ACTIVITIES.standard_relation,
            ACTIVITIES.standard_type,
            ACTIVITIES.standard_value,
            ACTIVITIES.standard_units,
            ASSAYS.assay_organism as strain,
            ASSAYS.chembl_id as assay_id
        FROM MOLECULE_DICTIONARY
        JOIN ACTIVITIES ON MOLECULE_DICTIONARY.molregno == ACTIVITIES.molregno
        JOIN ASSAYS ON ACTIVITIES.assay_id == ASSAYS.assay_id
        WHERE
            ASSAYS.assay_type == 'F'
            and ACTIVITIES.standard_value is not null
            and ACTIVITIES.standard_relation is not null
            and ACTIVITIES.standard_relation = '='
            and ACTIVITIES.standard_type = 'MIC'
    """

    assay_df = chembl_downloader.query(get_assays)
    assay_df = assay_df[assay_df['strain'].isin(bacterial_species)]
    assay_df.to_csv(f'../data/MIC/data_dump_{version}.tsv', sep='\t', index=False)

    # get_activity = """
    # SELECT
    #
    # FROM ACTIVITIES
    # WHERE and ACTIVITIES.standard_relation = '='
    # """
    #
    # act_df = chembl_downloader.query(get_activity)
    #
    # data_df = pd.merge(assay_df, act_df, on='id', how='left')
    #
    # get_compounds = """
    #     SELECT
    #
    #
    #     FROM MOLECULE_DICTIONARY
    #     """
    #
    # chem_df = chembl_downloader.query(get_compounds)
    #
    # data_df = pd.merge(data_df, chem_df, on='mol_id', how='left')
    # data_df = data_df[data_df['standard_type'] == 'MIC']
    # data_df.to_csv(f'../data/MIC/data_dump_{version}.tsv', sep='\t', index=False)


if __name__ == '__main__':
    get_data()
