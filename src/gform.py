# -*- coding: utf-8 -*-

"""Script to collect data from google form and store it locally."""


import pandas as pd

from github import Github
from github.NamedUser import NamedUser

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


def send_invite(
    mailing_list: list
):
    # TODO: Test if this works
    git = Github(GITHUB_TOKEN)
    repo = git.get_organization('ITeMP-temp').get_repo('AMR-KG')

    # for user in mailing_list:
    #
    #     github_user = Github.get_user('YojanaGadiya')
    #     print(github_user)
    #     # repo.add_to_collaborators(
    #     #
    #     # )


def save_data():
    data_list = get_data()

    # TODO: Fix code based on GitHub user name added to the survey.

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

    mails = []

    for data in data_list[2:]:
        values = {
                k: v
                for k, v in zip(col_names, data)
                if k
        }

        mails.append(values['Email'])

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
        }, index=[1])

        df = pd.concat([df, tmp_df], ignore_index=True)

    # send_invite(mailing_list=mails)

    print(df)


if __name__ == '__main__':
    save_data()

