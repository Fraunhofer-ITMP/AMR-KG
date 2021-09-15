# -*- coding: utf-8 -*-

import getopt
import os
import sys

import pandas as pd
from py2neo import Node, Relationship
from py2neo.database import Transaction
from tqdm import tqdm

from connection import populate_db
from constants import DATA_DIR, ENCODING


def map_data(
    data_df: pd.DataFrame
):
    # Map to institute name
    institute_dict = pd.read_csv(
        os.path.join(DATA_DIR, "AMR", "institute.csv"),
        dtype=str,
        index_col="id",
        encoding=ENCODING
    ).to_dict()["institute"]

    data_df["institute"] = data_df["institute"].map(institute_dict)

    # Map to project name
    project_dict = pd.read_csv(
        os.path.join(DATA_DIR, "AMR", "project.csv"),
        dtype=str,
        index_col="id",
        encoding=ENCODING
    ).to_dict()["project"]

    for i in ["project_1", "project_2"]:
        data_df[i] = data_df[i].map(project_dict)

    # Map to bacterial strain
    pathogen_dict = pd.read_csv(
        os.path.join(DATA_DIR, "AMR", "pathogen.csv"),
        dtype=str,
        index_col="id",
        encoding=ENCODING
    ).to_dict()["pathogen"]

    for i in ["pathogen_1", "pathogen_2", "pathogen_3"]:
        data_df[i] = data_df[i].map(pathogen_dict)

    # Map to skill set
    skill_dict = pd.read_csv(
        os.path.join(DATA_DIR, "AMR", "skill.csv"),
        dtype=str,
        index_col="id",
        encoding=ENCODING
    ).to_dict()["skill"]

    for i in ["skill_1", "skill_2", "skill_3", "skill_4"]:
        data_df[i] = data_df[i].map(skill_dict)

    return data_df


def add_nodes(
    data_df: pd.DataFrame,
    tx: Transaction
):
    """Add nodes specific to data """

    node_dict = {
        'Person': {},
        'Institute': {},
        'Skill': {},
        'Pathogen': {},
        'Project': {},
        'Year': {},
        'Chemical': {},
        'IC50': {},
        'Journal': {},
    }

    
    person_df = data_df[['first_name', 'last_name', 'email', 'orcid']]

    # Create person nodes
    for row in person_df.values:
        (
            first_name,
            last_name,
            email,
            orcid,
        ) = row
        person_property = {}

        if pd.notna(first_name) and pd.notna(last_name):
            name = first_name.capitalize() + ' ' + last_name.capitalize()
        else:
            continue

        if name in node_dict:
            continue

        person_property['name'] = name

        if pd.notna(email):
            person_property['email'] = email

        if pd.notna(orcid):
            person_property['orcid'] = orcid if 'https' in orcid else f' https://orcid.org/{orcid}'

        node_dict['Person'][name] = Node('Person', **person_property)
        tx.create(node_dict['Person'][name])

    # Create institute nodes
    institute_df = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'institute.csv'),
        usecols=['institute', 'link']
    )

    for institute_name, institute_page in institute_df.values:
        institute_property = {}

        if pd.notna(institute_name):
            institute_property['name'] = institute_name
            institute_property['link'] = institute_page

            node_dict['Institute'][institute_name] = Node('Institute', **institute_property)
            tx.create(node_dict['Institute'][institute_name])

    # Create project nodes
    project_df = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'project.csv'),
        dtype=str,
        index_col='id',
    )['project']

    for project_name in project_df.values:
        project_property = {}

        if pd.notna(project_name):
            project_property['name'] = project_name
            project_property[
                'link'] = f'https://www.imi.europa.eu/projects-results/project-factsheets/{project_name.lower()}'

            node_dict['Project'][project_name] = Node('Project', **project_property)
            tx.create(node_dict['Project'][project_name])

    # List of pathogens from data file
    interested_pathogen = []
    for i in data_df['pathogens'].values:
        if pd.notna(i):
            p = i.split(', ')
            interested_pathogen.extend(p)

    interested_pathogen = set(interested_pathogen)

    # Create pathogen node
    pathogen_df = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'pathogen.csv'),
        dtype=str,
    )
    pathogen_df = pathogen_df[pathogen_df['pathogen'].isin(interested_pathogen)]

    for pathogen_name, taxon_id in pathogen_df.values:
        pathogen_property = {}

        if pd.isna(pathogen_name):
            continue

        pathogen_property['name'] = pathogen_name
        pathogen_property['info'] = f'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id={taxon_id}'
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
        i
        for i in skill_df['category'].unique().tolist()
    }

    skill_def = {
        skill: definition
        for skill, definition in skill_df[['skill', 'definition']].values
        if pd.notna(definition)
    }

    skill_set_1 = skill_set_1.union(skill_set_2)

    for skill_name in skill_set_1:
        skill_property = {}

        if pd.isna(skill_name):
            continue

        skill_property['name'] = skill_name
        if skill_name in skill_def:
            skill_property['definition'] = skill_def[skill_name]

        node_dict['Skill'][skill_name] = Node('Skill', **skill_property)
        tx.create(node_dict['Skill'][skill_name])

    """ ADD MIC DATA """

    mic_df = pd.read_csv(
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
    mic_df = mic_df.loc[mic_df['strain'].isin(interested_pathogen)]

    chembl_to_node_map = {}

    if not mic_df.empty:
        # Create chemical nodes
        chemical_data = mic_df[['Molecule ChEMBL ID', 'NAME']]
        chemical_data = chemical_data.drop_duplicates()
        for chembl_id, name in chemical_data.values:
            chemical_property = {}

            if pd.notna(chembl_id):
                chemical_property['external link'] = f'https://www.ebi.ac.uk/chembl/compound_report_card/{chembl_id}/'

            if pd.notna(name):
                chemical_property['name'] = name

            node_dict['Chemical'][name] = Node('Chemical', **chemical_property)

            if pd.notna(chembl_id):  # To merge duplicates in chembl
                chembl_to_node_map[chembl_id] = Node('Chemical', **chemical_property)

            tx.create(node_dict['Chemical'][name])

        # Create pIC50 nodes
        ic50 = mic_df['pIC50'].unique().tolist()
        for val in ic50:
            ic50_property = {}

            if pd.notna(val):
                ic50_property['name'] = val
            node_dict['IC50'][val] = Node('IC50', **ic50_property)
            tx.create(node_dict['IC50'][val])

        # Add journal information
        journal_names = mic_df['Document Journal'].unique().tolist()
        journal_names = ['Assay test' if pd.isna(x) else x for x in journal_names]

        for name in journal_names:
            journal_property = {}

            if pd.notna(name):
                journal_property['name'] = name

            node_dict['Journal'][name] = Node('Journal', **journal_property)
            tx.create(node_dict['Journal'][name])

        # Add journal year
        publication_years = mic_df['Document Year'].unique().tolist()

        for year in publication_years:
            year_property = {}

            if pd.notna(year):
                year_property['year'] = year

                node_dict['Year'][year] = Node('Year', **year_property)
                tx.create(node_dict['Year'][year])

    """ ADD SPARK DATA """

    spark_df = pd.read_csv(
        os.path.join(DATA_DIR, 'SPARK', 'processed_mic_data.tsv'),
        sep='\t',
        dtype=str
    )
    spark_df = spark_df[spark_df['Curated & Transformed MIC Data: Species'].isin(interested_pathogen)]

    if not spark_df.empty:
        spark_chemicals = spark_df[['Compound Name', 'SMILES', 'Chembl #', 'pubchem']]
        spark_chemicals = spark_chemicals.drop_duplicates()

        for spark_id, smiles, chembl_id, pubchem_id in tqdm(spark_chemicals.values):

            chembl_flag = True

            chemical_property = {}

            if pd.notna(chembl_id):
                if chembl_id in chembl_to_node_map:
                    chembl_flag = False
                chemical_property['external link'] = f'https://www.ebi.ac.uk/chembl/compound_report_card/{chembl_id}/'
                external_id = chembl_id
            else:
                if pd.notna(pubchem_id):
                    pubchem_id = pubchem_id.split('.')[0]
                    if pubchem_id in chembl_to_node_map:
                        chembl_flag = False
                    chemical_property['external link'] = f'https://pubchem.ncbi.nlm.nih.gov/compound/{pubchem_id}'
                    external_id = pubchem_id
            if pd.notna(smiles):
                chemical_property['smiles'] = smiles

            chemical_property['name'] = spark_id

            if chembl_flag:
                node_dict['Chemical'][spark_id] = Node('Chemical', **chemical_property)
                tx.create(node_dict['Chemical'][spark_id])
            else:
                earlier_node = chembl_to_node_map[external_id]
                earlier_node.update({
                    'smiles': smiles,
                    'spark_id': spark_id
                }
                )
                tx.graph.push(earlier_node)

    return node_dict


def add_relations(
    tx: Transaction,
    df: pd.DataFrame,
    mic_df: pd.DataFrame,
    spark_df: pd.DataFrame,
    node_mapping_dict: dict
):
    """Add relations specific to AMR data. """

    for rows in tqdm(df.values, desc='Populating AMR graph'):
        (
            first_name,
            last_name,
            email,
            projects,
            institute_name,
            orcid,
            skills_1,
            skills_2,
            pathogens,
        ) = rows

        if pd.notna(first_name) and pd.notna(last_name):
            person_name = first_name.capitalize() + ' ' + last_name.capitalize()
        else:
            continue

        # Person - [WORKS_AT] -> Institute
        person_node = node_mapping_dict['Person'][person_name]
        institute_node = node_mapping_dict['Institute'][institute_name]
        works_at = Relationship(person_node, 'WORKS_AT', institute_node)
        tx.create(works_at)

        # Person - [IS_INVOLVED_IN] -> Project + Institute -[SUPERVISES] -> Project
        if pd.notna(projects):
            for project_name in projects.split(', '):
                project_node = node_mapping_dict['Project'][project_name]
                involved_in = Relationship(person_node, 'IS_INVOLVED_IN', project_node)
                tx.create(involved_in)

                supervises = Relationship(institute_node, 'SUPERVISES', project_node)
                tx.create(supervises)

        # Person - [HAS_SKILL] -> Skill
        if pd.notna(skills_1):
            for skill_1_name in skills_1.split(', '):
                skill_1_node = node_mapping_dict['Skill'][skill_1_name]
                has_skill = Relationship(person_node, 'HAS_SKILL', skill_1_node)
                tx.create(has_skill)

        if pd.notna(skills_2):
            for skill_2_name in skills_2.split(', '):
                skill_2_node = node_mapping_dict['Skill'][skill_2_name]
                has_skill = Relationship(person_node, 'HAS_SKILL', skill_2_node)
                tx.create(has_skill)

        # Person - [WORKS_WITH] -> Pathogen
        if pd.notna(pathogens):
            for pathogen_name in pathogens.split(', '):
                pathogen_node = node_mapping_dict['Pathogen'][pathogen_name]
                works_with = Relationship(person_node, 'WORKS_WITH', pathogen_node)
                tx.create(works_with)

    for row in tqdm(mic_df.values, desc='Adding MIC relations'):
        (
            strain,
            chembl_id,  # not needed
            chemical,
            pval,
            assay_id,
            doc_name,
            doc_year
        ) = row

        if strain not in node_mapping_dict['Pathogen']:  # Omitted as no one works with that strain
            continue

        bact_node = node_mapping_dict['Pathogen'][strain]
        chem_node = node_mapping_dict['Chemical'][chemical]

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
            p_node = node_mapping_dict['IC50'][pval]

            has_pic50 = Relationship(chem_node, 'HAS_pIC50', p_node)
            tx.create(has_pic50)

        if pd.notna(doc_name):
            journal_node = node_mapping_dict['Journal'][doc_name]

            found_in = Relationship(chem_node, 'FOUND_IN', journal_node)
            tx.create(found_in)

        if pd.notna(doc_year):
            year_node = node_mapping_dict['Year'][doc_year]

            in_year = Relationship(chem_node, 'IN_YEAR', year_node)
            tx.create(in_year)

    for row in tqdm(spark_df.values, desc='Adding SPARK relations'):
        (
            spark_id,
            smiles,
            chembl_id,
            pubmed_id,
            mic_val,
            specie,
            doi,
            pubchem_id,
            secondary_chembl_link,  # not used
        ) = row

        if specie not in node_mapping_dict['Pathogen']:  # Omitted as no one works with that strain
            continue

        bact_node = node_mapping_dict['Pathogen'][specie]
        try:
            chem_node = node_mapping_dict['Chemical'][spark_id]
        except KeyError:
            try:
                if pd.notna(chembl_id):
                    chem_node = node_mapping_dict['Chemical'][chembl_id]
                else:
                    chem_node = node_mapping_dict['Chemical'][pubchem_id.split('.')[0]]
            except KeyError:
                continue

        assay_property = {}

        if pd.notna(mic_val):
            assay_property['mic_values'] = mic_val

        if pd.notna(pubmed_id):
            assay_property['pubmed_id'] = f'https://pubmed.ncbi.nlm.nih.gov/{pubmed_id}/'

        if pd.notna(doi):
            assay_property['doi'] = doi

        assay_in = Relationship(
            bact_node,
            'ASSAY_IN',
            chem_node,
            **assay_property
        )
        tx.create(assay_in)


def add_skill_data(
    tx: Transaction,
    node_mapping_dict: dict
):
    """Add skill category connection to AMR KG. """

    skill_df = pd.read_csv(
        os.path.join(DATA_DIR, 'AMR', 'skill.csv'),
        usecols=[
            'skill',
            'category'
        ]
    )

    # skill_df['category'] = skill_df['category'].apply(lambda x: x + '_group')

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
        os.path.join(DATA_DIR, "AMR", "project.csv"),
        dtype=str,
        index_col="id",
    ).to_dict()["project"]

    institute_df = pd.read_csv(
        os.path.join(DATA_DIR, "AMR", "institute.csv"),
        usecols=['institute', 'projects'],

    )

    for row in tqdm(institute_df.values, desc='Populating institute projects'):
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
        print("amr-script -id <dbname>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-h":
            print("amr-script -d <dbname>")
            sys.exit()
        elif opt in ("-d", "--db"):
            db_name = arg

    tx = populate_db(db_name=db_name)

    # Load the form results
    df = pd.read_csv(
        f'{DATA_DIR}/AMR/survey_form.tsv',
        sep='\t',
        dtype=str,
        usecols=[
            'First name',
            'Surname',
            'Email',
            'Which project(s) are you involved in?',
            'Which research institute are you affiliated with?',
            'ORCID ID (Ex. https://orcid.org/0000-0002-7683-0452)',
            'Please select the most relevant expertises in DRUG DISCOVERY',
            'Please select the most relevant expertises in DRUG DEVELOPMENT',
            'Please choose the appropriate strain(s)'
        ],
        encoding=ENCODING
    )
    df = df.rename(columns={
        'First name': 'first_name',
        'Surname': 'last_name',
        'Email': 'email',
        'Which project(s) are you involved in?': 'projects',
        'Which research institute are you affiliated with?': 'institute',
        'ORCID ID (Ex. https://orcid.org/0000-0002-7683-0452)': 'orcid',
        'Please select the most relevant expertises in DRUG DISCOVERY': 'skill_1',
        'Please select the most relevant expertises in DRUG DEVELOPMENT': 'skill_2',
        'Please choose the appropriate strain(s)': 'pathogens',
    })

    # Load ChEMBL data
    mic_df = pd.read_csv(
        os.path.join(DATA_DIR, "MIC", "mic-data.tsv"),
        sep="\t",
        dtype=str,
        usecols=[
            "strain",
            "Molecule ChEMBL ID",
            "NAME",
            "pIC50",
            "Assay ChEMBL ID",
            "Document Year",
            "Document Journal"
        ],
        encoding=ENCODING
    )

    # Load SPARK data
    spark_df = pd.read_csv(
        os.path.join(DATA_DIR, 'SPARK', 'processed_mic_data.tsv'),
        sep='\t',
        dtype=str
    )

    # Add nodes
    node_map = add_nodes(data_df=df, tx=tx)

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
