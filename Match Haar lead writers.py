""" match writers from file given by a haar lead to existing writers in Sincere and show rooms where they reside,
addresses requested by year so we can tell if they should really be added to haar lead's room
"""

from multi_vals import is_found_in_another

import xlsxwriter 
from pathlib import Path
import pandas as pd
import numpy as np
# from datetime import datetime
import pymsgbox
# from bekutils import setup_loguru, autosize_xls_cols, bad_path_create, exit_yes_no, exit_yes, \
#     text_box, get_file_name, get_dir_name, check_ws_headers

DISPLAY_BLANK_NAME = False
RPT_PATH = Path('rpts')

SINCERE_REQUESTS = "/Users/Denise/Downloads/all-parent-campaigns-requests-2025-09-08.csv"  # requests from 1/1/2020
# to 9/9/2025
SINCERE_ALL_USERS = "/Users/Denise/Downloads/all-users-2025-09-10.csv"

INPUT_DATA = [
["Larry",
     "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Haar Larry writers for ROV ver 1.xlsx",
     "Name", "email",
     ],
["Jim",
     "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Jim Sincere "
     "Upload 20250817.xlsx",
     "Name", "E-mail",
     ],
]

# INPUT_DATA = [
#     ["Bob",
#      "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Bob Haar current writers and match to Sincere 7-26-2025.xlsx",
#      "Name", "Email",
#      ],
#     ["Judy",
#      "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Haar Judy writers 6.9.25.xlsx",
#      "Name", "Email",
#      ],
#     ["Gary",
#      "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Haar Gary writers 7-27-2025.xlsx",
#      "Name", "Email",
#      ],
#     ["Mary Jane",
#      "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Haar PoliticalMaryJaneWriters 7-28-2025.xlsx",
#      "Name", "Email Address",
#      ],
#     ["Larry",
#      "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Haar Larry writers for ROV ver 1.xlsx",
#      "Name", "email",
#      ],
#     ["Marlene",
#      "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Haar Marlene writers ROV ListForBrianKramer 7-29-20251.xlsx",
#      "NAME", "EMAIL",
#      ],
#     ["Jim",
#      "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Jim Sincere "
#      "Upload 20250817.xlsx",
#      "Name", "E-mail",
#      ]
# ]

## below begins programming code
SINCERE_REQUESTS = Path(SINCERE_REQUESTS)

SINCERE_ALL_USERS = Path(SINCERE_ALL_USERS)
# get date by concatenating last three items separated by '-'
SINCERE_ALL_USERS_DATE = '-'.join(SINCERE_ALL_USERS.stem.split('-')[-3:])


# noinspection SpellCheckingInspection
def format_as_matchname(name):
    """ format name in most standard fashion so match can be preformed """

    # TODO: use optional first name last name instead of combined

    try:
        # exception added for a name read as a float/nan
        matched_name = name.replace(' ', '').lower()
    except:
        matched_name = str(name)
    return matched_name


def read_org_data(org_xls, org_name_field, org_email_field):
    """ read bulk input file supplied by organizer and rename given name and email fields to standard."""

    org_data = pd.read_excel(org_xls, dtype=str)

    # trim all values so matches are accurate
    org_data_obj = org_data.select_dtypes('object')
    org_data[org_data_obj.columns] = org_data_obj.apply(lambda x: x.str.strip())

    org_data = org_data.rename(columns={org_name_field: 'name', org_email_field: 'new_email'})

    org_data['match_name'] = org_data['name'].apply(format_as_matchname)

    missing_names = org_data[org_data['match_name'] == '']
    if not missing_names.empty and DISPLAY_BLANK_NAME:
        # TODO write to log file and not console
        pymsgbox.alert(text=f"Some emails were blank in:\n\n   '{Path(org_xls).name}'\n\nThey will be removed.",
                       title='Check Console', button='OK')
        print()
        print(Path(org_xls).name)
        print(missing_names)

    org_data = org_data[org_data['match_name'] != '']

    return org_data, missing_names


# TODO: report on list of rows missing name or email; can't load into sincere

def read_sincere_requests(sincere_requests):
    """ read sincere requests file and rename some columns"""

    # TODO filter out or report missing emails and names
    sincere_data = pd.read_csv(sincere_requests, na_filter=False, dtype={'name': str, })
    sincere_data = sincere_data.rename(
        columns={'writer_name': 'name', 'writer_email': 'existing_email', 'org_name': 'room'})

    sincere_data['match_name'] = sincere_data['name'].apply(format_as_matchname)

    missing_name = sincere_data[sincere_data['match_name'] == ''][['match_name', 'existing_email']]
    if not missing_name.empty and DISPLAY_BLANK_NAME:
        # TODO write to log file and not console
        pymsgbox.alert(
            text=f"Some emails were blank in:\n\n   '{Path(sincere_requests).name}'\n\nThey will be removed.",
            title='Check Console', button='OK')
        print()
        print(Path(sincere_requests).name)
        print(missing_name)

    sincere_data = sincere_data[sincere_data['match_name'] != '']

    return sincere_data


def group_sincere_data(sincere_data, by_fields):
    """ create a row for each writer by year/room/email showing total addresses requested"""

    sincere_data['year'] = pd.to_datetime(sincere_data['created_at']).dt.year
    grouped_sincere_data = sincere_data.groupby(by_fields, dropna=False) \
        .agg({'addresses_count': ['sum', ]}).reset_index()

    grouped_sincere_data.columns = grouped_sincere_data.columns.get_level_values(0)
    return grouped_sincere_data


def merge_org_and_sincere_data(org_data, grouped_sincere_data):
    """ match the writers supplied by organizer by name to show if they are already in rooms and how many addresses
    they've written by year"""
    merged_data = org_data.merge(grouped_sincere_data, how='left', left_on='match_name', right_on='match_name',
                                 sort=False, suffixes=('_org', '_sincere',), copy=None, indicator=False, validate=None)

    merged_data = merged_data[~merged_data['room'].isna()]  # TODO: is this needed??
    merged_data = merged_data[merged_data['room'] != "National-Bob Haar"]
    merged_data['new_email'] = merged_data['new_email'].str.lower()
    merged_data['existing_email'] = merged_data['existing_email'].str.lower()

    # we know these are an existing writer because name and email match.
    merged_data['email_matches'] = np.where(merged_data['new_email'] == merged_data['existing_email'], 'Yes', 'No')
    merged_data = merged_data.reset_index(drop=True)
    return merged_data


def left_not_in_right(left, right, match_field):
    """ match two dataframes by a common column and return those in left not appearing in right """

    # link to merging / "in one not in other" code: https://stackoverflow.com/questions/53645882/pandas-merging-101
    merged = (left.merge(right, on=match_field, how='left', indicator=True, suffixes=('', '_right'))
     .query('_merge == "left_only"')
     .drop('_merge', axis=1))

    merged = merged[left.columns]

    return merged


def read_all_user_data(sincere_all_users):
    """ read in a sincere all_user csv"""
    # TODO: read and print active field

    all_user_data = pd.read_csv(sincere_all_users, na_filter=False, dtype={'name': str, })
    all_user_data = all_user_data.rename(
        columns={'email': 'existing_email', 'organization': 'room'})

    all_user_data['match_name'] = all_user_data['name'].apply(format_as_matchname)

    missing_name = all_user_data[all_user_data['match_name'] == ''][['match_name', 'existing_email']]
    if not missing_name.empty and DISPLAY_BLANK_NAME:
        # TODO write to log file and not console
        pymsgbox.alert(
            text=f"Some emails were blank in:\n\n   '{Path(sincere_all_users).name}'\n\nThey will be removed.",
            title='Check Console', button='OK')
        print()
        print(Path(sincere_all_users).name)
        print(missing_name)

    return all_user_data


def write_report_sheet(rpt_path, org_name, change_emails, duplicate_matchnames, merged_data, missing_names, all_haar):

    writer = pd.ExcelWriter(
        rpt_path / f"{org_name} batch load writer match with {SINCERE_ALL_USERS_DATE} user data.xlsx", engine='xlsxwriter')

    all_haar.to_excel(
        writer,
        index=False,
        sheet_name='all haar',
        startcol=1,
        columns=['name_org', 'is_active', 'email_matches', 'existing_email', 'room', 'year',
                 'addresses_count',
                 'new_email',])

    # Get the xlsxwriter workbook and worksheet objects
    workbook = writer.book
    worksheet = writer.sheets['all haar']

    worksheet.write(0, 0, 'band')  # column label, row_num, col_num (0-indexed)
    worksheet.write(1, 0, 0)  # seed of band column
    # worksheet.write_formula(2, 0, '=IF(C3<>C2,1-A2,A2)')  #

    # Write formulas to the 'ColC' column
    # Assuming data starts from row 2 (after header)
    for row_num in range(2, len(all_haar) + 1):  # Loop through rows, adjusting for 0-based index and header
        formula = f'=IF(B{row_num+1}<>B{row_num},1-A{row_num},A{row_num})'  # alternate 0/1 when column C changes,
        # '=IF(C3<>C2,1-A2,A2)'
        worksheet.write_formula(row_num, 0, formula)

    red_format = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})  # black on light red
    worksheet.conditional_format(f'$A$2:$J${len(all_haar)+1}', {
        'type': 'formula',
        'criteria': '=$A2=1',
        'format': red_format
    })
    worksheet.autofit()

    print("Excel file 'output_with_formulas.xlsx' created with formulas.")

    change_emails.to_excel(
        writer,
        sheet_name='change emails',
        index=True, columns=['name_org', 'email_matches', 'new_email', 'existing_email', 'room', 'year', 'addresses_count'])

    duplicate_matchnames.to_excel(
        writer,
        sheet_name='dupes only',
        index=True, columns=['name_org', 'email_matches', 'new_email', 'existing_email', 'room', 'year', 'addresses_count'])

    merged_data.to_excel(
        writer,
        sheet_name='all writers',
        index=True, columns=['name_org', 'email_matches', 'new_email', 'existing_email', 'room', 'year', 'addresses_count'])

    missing_names.to_excel(
        writer,
        sheet_name='missing_names',
        index=True, columns=['new_email', ])

    writer.close()


def main(input_data):
    """ loop through list of info for each organizers files and supply matched writer report for each"""

    sincere_requests = read_sincere_requests(SINCERE_REQUESTS)
    sincere_users = read_all_user_data(SINCERE_ALL_USERS)

    # writers who have never requested addresses so do not appear in request file
    users_not_in_requests = left_not_in_right(sincere_users, sincere_requests, 'match_name')

    # sincere_combined will have at least one record for each writer
    sincere_combined = pd.concat([sincere_requests, users_not_in_requests], ignore_index=True)
    sincere_combined = sincere_combined[['is_active'] + sincere_requests.columns.tolist()]

    sincere_combined = sincere_combined.sort_values(['match_name', ])

    # combine all record counts for each writer by year/room/email...
    grouped_sincere_data = group_sincere_data(sincere_combined, ['match_name', 'name', 'existing_email', 'room',
                                                                 'year', 'is_active'])

    for org_name, org_xls, org_name_field, org_email_field in input_data:
        org_data, missing_names = read_org_data(org_xls, org_name_field, org_email_field)

        # merged_data will contain only writers from organizer's file matched by name with past address request
        # counts or email/room for all matches
        merged_data = merge_org_and_sincere_data(org_data, grouped_sincere_data)

        all_haar = is_found_in_another(merged_data, 'room', group_column='match_name', filter_string='Haar')

        duplicate_matchnames = merged_data[merged_data.match_name.duplicated(keep=False)]
        haar_dupes = duplicate_matchnames.loc[
            (duplicate_matchnames['room'].str.contains('Haar', case=False, na=False)) &
            (duplicate_matchnames['email_matches'] == 'No')
             ]

        not_duplicate_matchnames = merged_data.drop_duplicates(subset='match_name', keep=False)

        # emails that should be changed: only show in org's room, name matches, email does not
        change_emails = not_duplicate_matchnames.loc[
            (not_duplicate_matchnames['room'].str.contains('Team ' + org_name, case=False, na=False)) &
            (not_duplicate_matchnames['email_matches'] == 'No')
             ]

        write_report_sheet(RPT_PATH, org_name, change_emails, duplicate_matchnames, merged_data, missing_names,
                           all_haar)


if __name__ == '__main__':
    main(INPUT_DATA)
