import random

import pandas as pd

from constants import FIRST_NAME, LAST_NAME, ORCID_LIST


def add_data_to_table(
    file_path: str,
    file_sep: str,
    data_count: int,
    max_data_count: int,
    new_col_name: str,
    output_file_path: str,
):
    df = pd.read_csv(
        file_path,
        dtype=str,
        sep=file_sep,
    )

    skill_ids = range(1, data_count + 1)

    skill_set = {idx: [] for idx in range(1, max_data_count + 1)}

    for i in range(df.shape[0]):
        skill_num = random.choice(range(1, max_data_count + 1))
        tmp_list = random.sample(skill_ids, skill_num)

        for idx in range(1, max_data_count + 1):
            try:
                skill_set[idx].append(str(tmp_list[idx - 1]))
            except IndexError:
                skill_set[idx].append("")

    for idx, val in skill_set.items():
        df[f"{new_col_name}_{idx}"] = val

    df.to_csv(output_file_path, index=False)


def get_anonymized_names(output_file_path: str):
    names = list(set(FIRST_NAME))
    second_names = LAST_NAME.split("\n")

    first_names = []
    last_names = []

    for i in range(len(names)):
        first_names.append(names[i].split()[0])
        last_names.append(second_names[i].split()[1])

    random.shuffle(last_names)

    # Get new names
    data = list(zip(first_names, last_names))[:35]

    with open(output_file_path, "w") as f:
        print("id,contact", file=f)
        for idx, n in enumerate(data):
            new_name = " ".join(n)
            print(f"{idx + 1},{new_name}", file=f)


def add_person_email(output_file_path: str, sep=","):
    df = pd.read_csv(
        output_file_path,
        sep=sep,
        dtype=str,
    )

    # Add email
    email = []
    for name in df['contact'].values:
        email.append(f"{name.split()[0]}.{name.split()[1]}@test.de")
    df["email"] = email

    df.to_csv(output_file_path, index=False)


def add_institute(output_file_path: str, sep=","):
    institute_list = list(range(1, 52))

    df = pd.read_csv(
        output_file_path,
        sep=sep,
        dtype=str,
    )

    institute = []
    for i in range(df.shape[0]):
        institute.append(random.choice(institute_list))

    df["institute"] = institute

    df.to_csv(output_file_path, index=False)


def add_orcid(output_file_path: str, sep=","):
    df = pd.read_csv(
        output_file_path,
        sep=sep,
        index_col="id",
        dtype=str,
    )

    orcid = []
    for i in range(df.shape[0]):
        orcid.append(random.choice(ORCID_LIST))

    df["orcid"] = orcid

    df.to_csv(output_file_path)


def add_project(output_file_path: str, sep=","):
    add_data_to_table(
        file_path=output_file_path,
        file_sep=sep,
        data_count=7,
        max_data_count=2,
        new_col_name="project",
        output_file_path=output_file_path,
    )


def add_pathogen(output_file_path: str, sep=","):
    add_data_to_table(
        file_path=output_file_path,
        file_sep=sep,
        data_count=23,
        max_data_count=3,
        new_col_name="pathogen",
        output_file_path=output_file_path,
    )


def add_skill(output_file_path: str, sep=","):
    add_data_to_table(
        file_path=output_file_path,
        file_sep=sep,
        data_count=110,
        max_data_count=4,
        new_col_name="skill",
        output_file_path=output_file_path,
    )


def main(
    output_file_path: str,
    has_institute: bool = False,
    has_email: bool = False,
    has_orcid: bool = False,
    has_pathogen: bool = False,
    has_skill: bool = False,
    has_project: bool = False,
):
    get_anonymized_names(output_file_path=output_file_path)

    if has_email:
        add_person_email(output_file_path=output_file_path)

    if has_institute:
        add_institute(output_file_path=output_file_path)

    if has_orcid:
        add_orcid(output_file_path=output_file_path)

    if has_project:
        add_project(output_file_path=output_file_path)

    if has_pathogen:
        add_pathogen(output_file_path=output_file_path)

    if has_skill:
        add_skill(output_file_path=output_file_path)


if __name__ == "__main__":
    main(
        output_file_path="../data/AMR/person.csv",
    )
