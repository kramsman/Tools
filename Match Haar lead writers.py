""" match writers from file given by a haar lead to existing writers in Sincere and show rooms where they reside,
addresses requested by year so we can tell if they should really be added to haar lead's room
"""

from pathlib import Path
import pandas as pd
import numpy as np
import pymsgbox
from bekutils import setup_loguru, autosize_xls_cols, bad_path_create, exit_yes_no, exit_yes, \
    text_box, get_file_name, get_dir_name, check_ws_headers

ORG_XLS = "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Jim Sincere Upload 20250817.xlsx"
SINCERE_ALL_USER = "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/all-users-2025-08-17.csv"
SINCERE_REQUESTS = "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads " \
            "7-2025/all-parent-campaigns-requests-2025-08-19.csv"  # requests from 1/1/2020 to 8/19/2025
OUTPUT_XLS = "Jim batch load writer match.xlsx"

ORG_NAME_FIELD = "Name"
ORG_EMAIL_FIELD = "E-mail"


## below begins programming code
ORG_XLS = Path(ORG_XLS)
SINCERE_ALL_USER = Path(SINCERE_ALL_USER)
SINCERE_REQUESTS = Path(SINCERE_REQUESTS)

def main_program(org_xls, sincere_requests):

    org_data = pd.read_excel(org_xls, dtype=str)
    org_data = org_data.rename(columns={ORG_NAME_FIELD: 'name', ORG_EMAIL_FIELD: 'new_email'})
    org_data['match_name'] = org_data['name'].str.replace(' ', '').str.lower()

    sincere_data = pd.read_csv(sincere_requests, na_filter=False)
    sincere_data = sincere_data.rename(columns={'writer_name': 'name', 'writer_email': 'existing_email', 'org_name': 'room'})
    sincere_data['match_name'] = sincere_data['name'].str.replace(' ', '').str.lower()
    # sincere_data['date'] = pd.to_datetime(sincere_data['created_at'])
    # sincere_data['year'] = sincere_data['date'].dt.year
    sincere_data['year'] = pd.to_datetime(sincere_data['created_at']).dt.year
    grouped_sincere_data = sincere_data.groupby(['match_name', 'name', 'existing_email', 'room', 'year'])\
        .agg({'addresses_count': ['sum', ]}).reset_index()

    grouped_sincere_data.columns = grouped_sincere_data.columns.get_level_values(0)
    merged_data = org_data.merge(grouped_sincere_data, how='left', left_on='match_name', right_on='match_name',
                    sort=False, suffixes=('_org', '_sincere', ), copy=None, indicator=False, validate=None)

    merged_data = merged_data[~merged_data['room'].isna()]
    merged_data = merged_data[merged_data['room'] != "National-Bob Haar"]
    merged_data['new_email'] = merged_data['new_email'].str.lower()
    merged_data['existing_email'] = merged_data['existing_email'].str.lower()

    merged_data['email_matches'] = np.where(merged_data['new_email'] == merged_data['existing_email'],'Yes', 'No')
    merged_data = merged_data.reset_index()

    merged_data.to_excel(OUTPUT_XLS, index=True, columns=['name_org', 'email_matches', 'new_email',
                                                           'existing_email', 'room', 'year', 'addresses_count'])

    # sincere_data = pd.read_csv(sincere_all_user, na_filter=False)
    # sincere_data = sincere_data.rename(columns={'email': 'existing_email', 'organization': 'room'})
    # sincere_data['match_name'] = sincere_data['name'].str.replace(' ', '').str.lower()
    # grouped_sincere_data = sincere_data.groupby(['name', 'existing_email', 'room']).agg({'zip': ['min', 'max']}).reset_index()


    # summarize address requests by year, room, by NAME OR EMAIL???? creating year, year_address_requested
    # match org_data to sincere_data by match_email for those in org_data into matched_names
    # field in sincere_data with matched_name????
    # print name, new_email, old_email, sincere_room, year_addresses

    a=1

if __name__ == '__main__':

    main_program(ORG_XLS, SINCERE_REQUESTS)
