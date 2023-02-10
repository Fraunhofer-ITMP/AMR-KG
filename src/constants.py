# -*- coding: utf-8 -*-

# Data paths
# DATA_DIR = "~/data/"
DATA_DIR = '../data'

# Neo4J connectors
# ADMIN_NAME = "neo4j"
ADMIN_NAME = 'yojana'
ADMIN_PASS = 'root'
# ADMIN_PASS = "neo4jbinder"
URL = "bolt://localhost:7687"
ENCODING = 'ISO-8859-1'
ENGINE = "python"

# DrugCentral pathogen mapper
PATHOGEN_MAPPER = {
    'Escherichia coli': 'Escherichia coli',
    'Escherichia coli (strain K12)': 'Escherichia coli',
    'Helicobacter pylori (strain ATCC 700392 / 26695)': 'Helicobacter pylori',
    'Mycobacterium tuberculosis (strain ATCC 25618 / H37Rv)': 'Mycobacterium tuberculosis',
    'Pseudomonas aeruginosa (strain ATCC 15692 / DSM 22644 / CIP 104116 / JCM 14847 / LMG 12228 / 1C / PRS 101 / PAO1)': 'Pseudomonas aeruginosa',
    'Helicobacter pylori': 'Helicobacter pylori',
    'Pseudomonas aeruginosa': 'Pseudomonas aeruginosa',
    'Shigella dysenteriae': 'Shigella sp.',
    'Plasmodium falciparum': 'Plasmodium falciparum',
    'Pseudomonas aeruginosa (strain ATCC 15692 / PAO1 / 1C / PRS 101 / LMG 12228)': 'Pseudomonas aeruginosa',
    'Klebsiella pneumoniae': 'Klebsiella pneumoniae',
    'Acinetobacter baumannii': 'Acinetobacter baumannii',
    'Plasmodium falciparum (isolate 3D7)': 'Plasmodium falciparum',
    'Salmonella typhi': 'Salmonella',
    'Haemophilus influenzae (strain ATCC 51907 / DSM 11121 / KW20 / Rd)': 'Haemophilus influenzae',
    'Streptococcus pyogenes serotype M1': 'Streptococcus pyogenes',
    'Haemophilus influenzae': 'Haemophilus influenzae',
    'Streptococcus pyogenes': 'Streptococcus pyogenes',
    'Streptococcus pyogenes serotype M4 (strain MGAS10750)': 'Streptococcus pyogenes',
    'Mycobacterium tuberculosis (strain CDC 1551 / Oshkosh)': 'Mycobacterium tuberculosis',
    'Neisseria gonorrhoeae': 'Neisseria gonorrhoeae',
    'Streptococcus pneumoniae serotype 4 (strain ATCC BAA-334 / TIGR4)': 'Streptococcus pneumoniae',
    'Escherichia coli O157:H7': 'Escherichia coli',
    'Streptococcus pneumoniae': 'Streptococcus pneumoniae',
    'Enterococcus faecium': 'Enterococcus faecium',
    'Staphylococcus aureus (strain Newman)': 'Staphylococcus aureus',
    'Mycobacterium tuberculosis': 'Mycobacterium tuberculosis',
    'Mycobacterium avium': 'Mycobacterium avium complex',
    'Plasmodium falciparum (isolate FcB1 / Columbia)': 'Plasmodium falciparum',
    'Helicobacter pylori (strain HPAG1)': 'Helicobacter pylori',
    'Staphylococcus aureus (strain MRSA252)': 'Staphylococcus aureus',
    'Clostridioides difficile': 'Clostridium difficile',
    'Klebsiella pneumoniae subsp. pneumoniae (strain ATCC 700721 / MGH 78578)': 'Klebsiella pneumoniae',
    'Escherichia coli DEC1B': 'Escherichia coli',
    'Acinetobacter baumannii (strain ATCC 19606 / DSM 30007 / CIP 70.34 / JCM 6841 / NBRC 109757 / NCIMB 12457 / NCTC 12156 / 81)': 'Acinetobacter baumannii'
}
