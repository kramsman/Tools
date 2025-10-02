""" match writers from file given by a haar lead to existing writers in Sincere and show rooms where they reside,
addresses requested by year so we can tell if they should really be added to haar lead's room
"""

from multi_vals import is_found_in_another

from pathlib import Path
import pandas as pd
import numpy as np
# from datetime import datetime
import pymsgbox
# from bekutils import setup_loguru, autosize_xls_cols, bad_path_create, exit_yes_no, exit_yes, \
#     text_box, get_file_name, get_dir_name, check_ws_headers

FILTER_ONE_NAME = False  # only name below for testing
NAME_TO_FILTER = 'Susan Bennett'  # format_as_matchname converts to lower with spaces removed below

DISPLAY_BLANK_NAME = False  # print missing names to console
RPT_PATH = Path('rpts')

SINCERE_REQUESTS = "/Users/Denise/Downloads/all-parent-campaigns-requests-2025-09-08.csv"  # requests from 1/1/2020
# to 9/9/2025
SINCERE_ALL_USERS = "/Users/Denise/Downloads/all-users-2025-09-10.csv"

ROOMS_TO_FILTER_OUT = ['National-Bob Haar', 'ZZZZZZ National Bob Haar - Team Casey']  # list of names ignored in reports

if False:
    INPUT_DATA = [
        ["Larry",
         "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Haar Larry writers for ROV ver 1.xlsx",
         "Name", "email",
         ],
        ["Jim",
         "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Jim Sincere "
         "Upload 20250817.xlsx",
         "Name", "E-mail",
         ]
    ]
else:
    INPUT_DATA = [
        ["Bob",
         "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Bob Haar current writers and match to Sincere 7-26-2025.xlsx",
         "Name", "Email",
         ],
        ["Judy",
         "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Haar Judy writers 6.9.25.xlsx",
         "Name", "Email",
         ],
        ["Gary",
         "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Haar Gary writers 7-27-2025.xlsx",
         "Name", "Email",
         ],
        ["Mary Jane",
         "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Haar PoliticalMaryJaneWriters 7-28-2025.xlsx",
         "Name", "Email Address",
         ],
        ["Larry",
         "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Haar Larry writers for ROV ver 1.xlsx",
         "Name", "email",
         ],
        ["Marlene",
         "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Haar Marlene writers ROV ListForBrianKramer 7-29-20251.xlsx",
         "NAME", "EMAIL",
         ],
        ["Jim",
         "/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/Other/WriterLists/Haar leads 7-2025/Jim Sincere "
         "Upload 20250817.xlsx",
         "Name", "E-mail",
         ]
    ]

## below begins programming code
SINCERE_REQUESTS = Path(SINCERE_REQUESTS)

SINCERE_ALL_USERS = Path(SINCERE_ALL_USERS)
# get date by concatenating last three items separated by '-'
SINCERE_ALL_USERS_DATE = '-'.join(SINCERE_ALL_USERS.stem.split('-')[-3:])


# noinspection SpellCheckingInspection
def format_as_matchname(name):
    """ format name in most standard fashion so match can be preformed """

    try:
        # exception added for a name read as a float/nan
        matched_name = name.replace(' ', '').lower()
    except:
        matched_name = str(name)
    return matched_name


def read_org_file(org_xls, org_name_field, org_email_field):
    """ read bulk input file supplied by organizer and rename given name and email fields to standard."""

    org_data = pd.read_excel(org_xls, dtype=str)

    # trim all values so matches are accurate
    org_data_obj = org_data.select_dtypes('object')
    org_data[org_data_obj.columns] = org_data_obj.apply(lambda x: x.str.strip())

    org_data = org_data.rename(columns={org_name_field: 'name', org_email_field: 'new_email'})

    org_data['match_name'] = org_data['name'].apply(format_as_matchname)
    if FILTER_ONE_NAME:
        org_data = org_data[org_data['match_name'] == NAME_TO_FILTER]  # 'barbaralewis']

    missing_names = org_data[org_data['match_name'] == '']
    if not missing_names.empty and DISPLAY_BLANK_NAME:
        pymsgbox.alert(text=f"Some emails were blank in:\n\n   '{Path(org_xls).name}'\n\nThey will be removed.",
                       title='Check Console', button='OK')
        print()
        print(Path(org_xls).name)
        print(missing_names)

    org_data = org_data[org_data['match_name'] != '']

    return org_data, missing_names


def read_sincere_requests(sincere_requests):
    """ read sincere requests file and rename some columns"""

    sincere_data = pd.read_csv(sincere_requests, na_filter=False, dtype={'name': str, })
    sincere_data = sincere_data.rename(
        columns={'writer_name': 'name', 'writer_email': 'existing_email', 'org_name': 'room'})

    sincere_data['match_name'] = sincere_data['name'].apply(format_as_matchname)
    if FILTER_ONE_NAME:
        sincere_data = sincere_data[sincere_data['match_name'] == NAME_TO_FILTER]

    missing_name = sincere_data[sincere_data['match_name'] == ''][['match_name', 'existing_email']]
    if not missing_name.empty and DISPLAY_BLANK_NAME:
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


def merge_org_and_sincere_groups(*, org_data, grouped_sincere_data):
    # FIXED: losing grouped data
    # All users 2025-9-10 shows:
    # NC-Entire State
    # National Bob Haar - Team Larry
    # National-Bob Haar
    # all writers in report shows only 2:
    # National Bob Haar - Team Larry	2025	100	dwraynes@gmail.com
    # NC-Entire State		0	dwraynes@gmail.com

    """ match the writers supplied by organizer by name to show if they are already in rooms and how many addresses
    they've written by year """
    merged_data = org_data.merge(grouped_sincere_data, how='left', left_on='match_name', right_on='match_name',
                                 sort=False, suffixes=('_org', '_sincere',), copy=None, indicator=False, validate=None)

    merged_data = merged_data[~merged_data['room'].isna()]

    merged_data['new_email'] = merged_data['new_email'].str.lower()
    merged_data['existing_email'] = merged_data['existing_email'].str.lower()

    # we know this is an existing writer because old and new emails match for given name
    merged_data['email_matches'] = np.where(merged_data['new_email'] == merged_data['existing_email'], 'Yes', 'No')
    merged_data = merged_data.reset_index(drop=True)
    return merged_data


def left_not_in_right(*, left, right, match_field_list):
    """ match two dataframes by a common column and return those in left not appearing in right
    """

    # link to merging / "in one not in other" code: https://stackoverflow.com/questions/53645882/pandas-merging-101
    left_not_in_right = (left.merge(right, on=match_field_list, how='left', indicator=True, suffixes=('', '_right'))
     .query('_merge == "left_only"')
     .drop('_merge', axis=1))

    left_not_in_right = left_not_in_right[left.columns]

    return left_not_in_right


def read_all_user_data(sincere_all_users):
    """ read in a sincere all_user csv"""

    all_user_data = pd.read_csv(sincere_all_users, na_filter=False, dtype={'name': str, })
    all_user_data = all_user_data.rename(
        columns={'email': 'existing_email', 'organization': 'room'})

    all_user_data['match_name'] = all_user_data['name'].apply(format_as_matchname)
    all_user_data = all_user_data[all_user_data['is_active'] == True]
    if FILTER_ONE_NAME:
        all_user_data = all_user_data[all_user_data['match_name'] == NAME_TO_FILTER]

    missing_name = all_user_data[all_user_data['match_name'] == ''][['match_name', 'existing_email']]
    if not missing_name.empty and DISPLAY_BLANK_NAME:
        pymsgbox.alert(
            text=f"Some emails were blank in:\n\n   '{Path(sincere_all_users).name}'\n\nThey will be removed.",
            title='Check Console', button='OK')
        print()
        print(Path(sincere_all_users).name)
        print(missing_name)

    return all_user_data


def write_report(rpt_path, org_name, org_filename, *, change_emails, cross_rooms, overlap_w_haar, missing_names,
                 all_writers):
    """ write all sheets into workbook for report dfs
    """

    # FIXME: add report of multiple emails & multiple names in org file (mary jane  file / nancy beaudet - (like
    #  missing name code)
    # FIXME: sort by name_org, email_matches, new_email, existing_email
    # FIXME: why does cathy moratto show on change email list when email is the the same?  Capital name?  Why
    #  Elizabeth Hulton?
    # FIXME: Esther mac in Gary

    writer = pd.ExcelWriter(
        rpt_path / f"{org_name} batch load writer match with {SINCERE_ALL_USERS_DATE} user data.xlsx", engine='xlsxwriter')

    def write_banded(*, df, sheetname, field_list, title1, hidesheet=False):
        """ print sheet in specific format
        """

        def index_to_col(column_int):
            """ converts an excel column number to corresponding letter """
            start_index = 1  # it can start either at 0 or at 1
            letter = ''
            while column_int > 25 + start_index:
                letter += chr(65 + int((column_int - start_index) / 26) - 1)
                column_int = column_int - (int((column_int - start_index) / 26)) * 26
            letter += chr(65 - start_index + (int(column_int)))
            return letter

        header_row = 2  # zero indexed
        df.to_excel(
            writer,
            index=False,
            sheet_name=sheetname,
            startrow=header_row,
            startcol=1,
            columns=field_list)

        # Get the xlsxwriter workbook and worksheet objects
        workbook = writer.book
        worksheet = writer.sheets[sheetname]
        worksheet.set_landscape()
        worksheet.set_margins(left=.25, right=.25, top=.25, bottom=.25)
        worksheet.fit_to_pages(1, 99)
        worksheet.print_black_and_white()

        worksheet.write(header_row, 0, 'band')  # column label, row_num, col_num (0-indexed)
        worksheet.write(header_row+1, 0, 0)  # seed of band column
        # worksheet.write_formula(2, 0, '=IF(C3<>C2,1-A2,A2)')  #

        # Write formulas down column assuming data starts from row after header
        for row_num in range(header_row+2, len(df) + 3):  # Loop through rows, adjusting for 0-based index and
            # header
            formula = f'=IF(B{row_num+1}<>B{row_num},1-A{row_num},A{row_num})'  # alternate 0/1 when column C changes,
            # '=IF(C3<>C2,1-A2,A2)'
            worksheet.write_formula(row_num, 0, formula)

        col = index_to_col(len(field_list)+1)
        red_format = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})  # black on light red
        worksheet.conditional_format(f'$A${header_row+1}:${col}${len(df)+ header_row+1}', {
            'type': 'formula',
            'criteria': f'=$A{header_row+1}=1',
            'format': red_format
        })
        worksheet.autofit()
        worksheet.write(0, 0, title1)
        if hidesheet:
            worksheet.hide()

    if not change_emails.empty:
        write_banded(df=change_emails, sheetname='change emails',
                     field_list=['name_org', 'email_matches', 'new_email', 'existing_email', 'room', 'year',
                                 'addresses_count'],
                     title1=f"'{org_filename}' batch load writer match with '{SINCERE_ALL_USERS_DATE} user data.xlsx'",
                     hidesheet=False)

    if not cross_rooms.empty:
        write_banded(df=cross_rooms, sheetname='cross rooms',
                     field_list=['name_org', 'email_matches', 'new_email', 'existing_email', 'room', 'year',
                                 'addresses_count'],
                     title1=f"'{org_filename}' batch load writer match with '{SINCERE_ALL_USERS_DATE} user data.xlsx'",
                     hidesheet=False)

    if not overlap_w_haar.empty:
        write_banded(df=overlap_w_haar, sheetname='overlap_w_haar',
                     field_list=['name_org', 'email_matches', 'new_email', 'existing_email', 'room', 'year',
                                 'addresses_count'],
                     title1=f"'{org_filename}' batch load writer match with '{SINCERE_ALL_USERS_DATE} user data.xlsx'",
                     hidesheet=True)

    if not missing_names.empty:
        write_banded(df=missing_names, sheetname='missing_names', field_list=['new_email', ],
                     title1=f"'{org_filename}' batch load writer match with '{SINCERE_ALL_USERS_DATE} user data.xlsx'",
                     hidesheet=False)

    if not all_writers.empty:
        write_banded(df=all_writers, sheetname='all occurances',
                     field_list=['name_org', 'is_active', 'email_matches', 'existing_email', 'room', 'year',
                                 'addresses_count', 'new_email', ],
                     title1=f"'{org_filename}' batch load writer match with '{SINCERE_ALL_USERS_DATE} user data.xlsx'",
                     hidesheet=True)

    writer.close()


def main(input_data):
    """ loop through list of info for each organizers files and supply matched writer report for each"""

    # all users in system
    sincere_users = read_all_user_data(SINCERE_ALL_USERS)

    # address counts showing activity in which room; users who never requested are missing
    sincere_requests = read_sincere_requests(SINCERE_REQUESTS)

    # identify writers who have never requested addresses to merge in later
    users_not_in_requests = left_not_in_right(left=sincere_users, right=sincere_requests,
                                              match_field_list=['match_name', 'room', 'existing_email'])

    # add writers with no requests to request info
    sincere_combined = pd.concat([sincere_requests, users_not_in_requests], ignore_index=True)
    # only keep columns from request files along with 'is_active' (from user file)
    sincere_combined = sincere_combined[['is_active'] + sincere_requests.columns.tolist()]

    sincere_combined = sincere_combined.sort_values(['match_name', ])

    # summarize address counts by name/year/room/email to examine later by name
    # this will be merged with individual bulk files supplied by orgs for comparison
    grouped_sincere_data = group_sincere_data(sincere_combined, ['match_name', 'name', 'existing_email', 'room',
                                                                 'year', 'is_active'])

    ####
    #### process each organizer's file and report
    for org_name, org_xls, org_name_field, org_email_field in input_data:  # input_data is list of org's file and info

        # get the org filename from the whole path to use as identifier in reports
        org_file = Path(org_xls).name

        # read org's bulk input file supplying their fields used for name and email (different between orgs)
        # create an error file of missing writer names (Sincere will ignore and not load)
        all_writers, missing_names = read_org_file(org_xls, org_name_field, org_email_field)

        # matching and lookup must be done by name because writer can have different emails within or across rooms
        # merged_data will contain only writers from org's file matched by name with past request counts or email/room
        merged_data = merge_org_and_sincere_groups(org_data=all_writers, grouped_sincere_data=grouped_sincere_data)

        merged_data = merged_data.sort_values(['match_name', ])

        ####### create report files
        # df with only groups of records where 'Team {org_name}' is in at least one room
        # emails that should be changed: only show in org's room with name matching but different email
        change_emails = is_found_in_another(df_func=merged_data, check_field='room', id_field='match_name',
                                            filter_string=f'Team {org_name}', one_room=True,
                                            single_row_incl=True,
                                            filter_out=ROOMS_TO_FILTER_OUT)
        if not change_emails.empty:
            change_emails = change_emails[(change_emails['email_matches'] == 'No') | (change_emails['_row_count'] > 1)]
            # email_matches if one does across rows

        all_writers = is_found_in_another(df_func=merged_data, check_field='room', id_field='match_name',
                                          filter_string=f'Team {org_name}', one_room=False,
                                          filter_out=[])

        cross_rooms = is_found_in_another(df_func=merged_data, check_field='room', id_field='match_name',
                                          filter_string=f'Team {org_name}', one_room=False,
                                          filter_out=ROOMS_TO_FILTER_OUT)

        overlap_w_haar = is_found_in_another(df_func=merged_data, check_field='room', id_field='match_name',
                                             filter_string=f'Team {org_name}', one_room=False,
                                             filter_out=[])

        write_report(RPT_PATH, org_name, org_file, change_emails=change_emails, cross_rooms=cross_rooms,
                     overlap_w_haar=overlap_w_haar, missing_names=missing_names, all_writers=all_writers)


if __name__ == '__main__':
    NAME_TO_FILTER = format_as_matchname(NAME_TO_FILTER)

    main(INPUT_DATA)
