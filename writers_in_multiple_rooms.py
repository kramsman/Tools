"""Report writers who appear in multiple organizations.

Input:  Sincere all-users CSV (selected via file dialog)
Output: Excel report in rpts/ listing each writer in multiple orgs with their org list
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd
from uvbekutils import select_file
from xlsxwriter import Workbook

RPT_PATH = Path('rpts')
DOWNLOADS = Path.home() / 'Downloads'

# organizations whose names contain any of these substrings (case-insensitive) are excluded
EXCLUDE_ORG_SUBSTRINGS = ['National Bob Haar', 'National-Bob Haar', 'test', 'training', 'xxx']


def load_writers(csv_path: str | Path) -> pd.DataFrame:
    """Read all-users CSV, filter to active writers, and return relevant fields.

    Args:
        csv_path (str | Path): Path to the Sincere all-users CSV file.

    Returns:
        pd.DataFrame: DataFrame with columns name, email, organization
            for active writers (role='writer', is_active=True), excluding
            organizations matching any substring in EXCLUDE_ORG_SUBSTRINGS.
    """

    df = pd.read_csv(csv_path, na_filter=False, dtype={'name': str})
    df = df[['name', 'email', 'role', 'is_active', 'organization']]
    df = df[(df['role'].str.strip().str.lower() == 'writer') & (df['is_active'] == True)]
    df = df[['name', 'email', 'organization']].copy()

    # exclude organizations whose name contains any of the configured substrings
    mask = df['organization'].str.contains('|'.join(EXCLUDE_ORG_SUBSTRINGS), case=False, na=False)
    return df[~mask]


def find_multi_org_writers(df: pd.DataFrame) -> pd.DataFrame:
    """Return one row per writer+org for writers appearing in more than one organization.

    Args:
        df (pd.DataFrame): DataFrame with columns email, name, organization.

    Returns:
        pd.DataFrame: Subset of df sorted by email, name, organization,
            containing only writers whose email appears in multiple organizations.
    """

    org_counts = df.groupby('email')['organization'].nunique()
    multi_org_emails = org_counts[org_counts > 1].index
    result = df[df['email'].isin(multi_org_emails)].copy()
    return result.sort_values(['email', 'name', 'organization']).reset_index(drop=True)


def write_report(df: pd.DataFrame, csv_path: str | Path, total_writers: int) -> None:
    """Write multi-org writers report to Excel with alternating row bands grouped by email.

    Output file is written to RPT_PATH with a filename derived from the date
    portion of the input filename. Columns are ordered email, name, organization
    with col A used as a banding helper column.

    Args:
        df (pd.DataFrame): Writers in multiple orgs with columns email, name, organization.
        csv_path (str | Path): Path to the source CSV; used to derive the output
            filename date suffix and the report title.
        total_writers (int): Total number of unique active writers after org filtering,
            shown as a summary count above the data.
    """

    date_str = '-'.join(Path(csv_path).stem.split('-')[-3:])
    out_path = RPT_PATH / f"writers_in_multiple_orgs_{date_str}.xlsx"
    multi_org_count = df['email'].nunique()

    with pd.ExcelWriter(out_path, engine='xlsxwriter') as writer:
        header_row = 6  # zero-indexed; row 0 = title, 1 = blank, 2-3 = counts, 4 = filter, 5 = blank, 6 = header
        # email in col B so banding formula (which compares col B) groups by email
        df.to_excel(writer, index=False, sheet_name='multiple orgs',
                    startrow=header_row, startcol=1,
                    columns=['email', 'name', 'organization'])

        workbook = writer.book
        worksheet = writer.sheets['multiple orgs']
        worksheet.set_landscape()
        worksheet.set_margins(left=.25, right=.25, top=.25, bottom=.25)
        worksheet.fit_to_pages(1, 99)

        # title in col B; blank line; summary counts; filter list
        worksheet.write(0, 1, f"Writers in multiple organizations — {Path(csv_path).name}")
        worksheet.write(2, 1, 'Total active writers (after filtering):')
        worksheet.write(2, 2, total_writers)
        worksheet.write(3, 1, 'Writers in multiple organizations:')
        worksheet.write(3, 2, multi_org_count)
        worksheet.write(4, 1, 'Excluded organizations containing:')
        worksheet.write(4, 2, ', '.join(EXCLUDE_ORG_SUBSTRINGS))

        worksheet.set_column('A:A', None, None, {'hidden': True})

        # col A: band seed and formulas that alternate 0/1 each time email (col B) changes
        worksheet.write(header_row, 0, 'band')
        worksheet.write(header_row + 1, 0, 0)
        for row_num in range(header_row + 2, len(df) + header_row + 1):
            formula = f'=IF(B{row_num + 1}<>B{row_num},1-A{row_num},A{row_num})'
            worksheet.write_formula(row_num, 0, formula)

        red_fmt = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        worksheet.conditional_format(
            f'$B${header_row + 1}:$D${len(df) + header_row + 1}',
            {'type': 'formula', 'criteria': f'=$A{header_row + 1}=1', 'format': red_fmt})

        worksheet.autofit()

    print(f"Report written to: {out_path}")
    print(f"  {df['email'].nunique()} writers across {len(df)} rows")


def main() -> None:
    """Select input file, find writers in multiple orgs, and write report.

    Prompts the user to select a Sincere all-users CSV via a file dialog,
    filters to active writers in multiple organizations, and writes an Excel
    report to the rpts/ directory.
    """
    csv_path = select_file(
        title='Select Sincere all-users CSV',
        start_dir=str(DOWNLOADS),
        files_like='all-user*.csv')

    if csv_path is None:
        print('No file selected.')
        return

    df = load_writers(csv_path)
    total_writers = df['email'].nunique()
    multi_org = find_multi_org_writers(df)

    if multi_org.empty:
        print('No writers found in multiple organizations.')
        return

    write_report(multi_org, csv_path, total_writers)


if __name__ == '__main__':
    main()
