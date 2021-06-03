# -*- coding: utf-8 -*-

"""Script to collect data from google form and store it locally."""


import pandas as pd

from github import Github
import requests

import gspread
from oauth2client.service_account import ServiceAccountCredentials


scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    'credential.json',
    scope
)
client = gspread.authorize(creds)


def get_data():
    sheet = client.open_by_url(
        url='https://docs.google.com/spreadsheets/d/171YA-Tx0RTWVlNQrllDr3cyiN572C4MgQrwryIehk-I/edit?resourcekey#gid=1670450062'
    ).get_worksheet(index=0)

    return sheet.get_all_values()


def send_organization_invite(
    mailing_list: list
) -> None:
    git = Github(GITHUB_TOKEN)

    repo = git.get_organization('ITeMP-temp')

    h = {
        'Content-type': 'application/json',
        'Accept': 'application/vnd.github.v3+json'
    }

    for mail in mailing_list:
        d = requests.post(
            f'{repo.url}/invitations',
            json={
                'email': mail,
            },
            headers=h,
            auth=('YojanaGadiya', GITHUB_TOKEN),
        )

        if d.status_code == 201:
            print(f'Awaiting response from {mail}')

    return None


def send_repo_invite(
    username_list: list,
) -> None:
    git = Github(GITHUB_TOKEN)

    repo = git.get_organization('ITeMP-temp').get_repo('AMR-KG')

    for name in username_list:
        if name:
            repo.add_to_collaborators(
                collaborator=name,
                permission='pull',
            )

    return None


def save_data():
    data_list = get_data()

    df = pd.DataFrame(
        columns=[
            'timestamp',
            'first_name',
            'surname',
            'email',
            'projects',
            'institute',
            'discovery_skills',
            'pre_clinical_skills',
            'clinical_skills',
            'pathogens',
            'orcid'
        ]
    )

    col_names = data_list[0]

    username_list = []

    for data in data_list[2:]:
        values = {
                k: v
                for k, v in zip(col_names, data)
                if k
        }

        username_list.append(values['GitHub UserName'])

        tmp_df = pd.DataFrame({
            'timestamp': values['Timestamp'],
            'first_name': values['First name'],
            'surname': values['Surname'],
            'email': values['Email'],
            'projects': values['Which project(s) are you involved in?'],
            'institute': values['Which research institute are you affiliated with?'],
            'discovery_skills': values['Please select the most relevant skill(s) in DISCOVERY'],
            'pre_clinical_skills': values['Please select the most relevant skill(s) in PRE-CLINICAL DEVELOPMENT'],
            'clinical_skills': values['Please select the most relevant skill(s) in CLINICAL DEVELOPMENT'],
            'pathogens': values['Please choose the appropriate strain(s)'],
            'orcid': values['ORCID ID (Ex. https://orcid.org/0000-0002-7683-0452)'],
            'username': values['GitHub UserName']
        }, index=[1])

        df = pd.concat([df, tmp_df], ignore_index=True)

    send_repo_invite(username_list=username_list)

    # pd.set_option('display.max_columns', None)
    # print(df)


if __name__ == '__main__':
    save_data()
