
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    'C:/Users/Yojana.Gadiya/AppData/Roaming/gspread/credential.json',
    scope
)
client = gspread.authorize(creds)


def get_data():
    sheet = client.open_by_url(
        url='https://docs.google.com/spreadsheets/d/171YA-Tx0RTWVlNQrllDr3cyiN572C4MgQrwryIehk-I/edit?resourcekey#gid=1670450062'
    ).get_worksheet(index=0)

    return sheet.get_all_values()


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


if __name__ == '__main__':
    get_data()

