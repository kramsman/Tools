""" return records matched by common field in first df but not second"""

import pandas as pd


def df_issubset(item, list_from_df_element):
    """ issubset function when list is an element of a dataframe
    (I think doesn't work because list is returned as series"""

    # normal_list = list_from_df_element
    if not isinstance(item, list):
        item_as_list = [item]
    else:
        item_as_list = item

    if not isinstance(list_from_df_element, list):
    # if not isinstance(normal_list, list):
        is_subset = False
    else:
        is_subset = set(item_as_list).issubset(list_from_df_element)
        # is_subset = set(item_as_list).issubset(normal_list)

    return is_subset


def is_found_in_another(df, occurance_col, group_column=None, filter_string=None, solo_occ=False):
    """ returns records with col_name value found in other records
    """

    # FIXME: add ability to keep single rows - maybe desired for solo?

    if group_column is None:  # this is no group. need to set up simple case with list normally created via groupby
        group_column = occurance_col

    # Group by 'key_column' and aggregate 'value_column' into a list
    df_groupby = df.groupby(group_column) \
        .agg(_col_list=(occurance_col, list),
             _row_count=(occurance_col, 'count')
             )\
        .reset_index().query("_row_count > 1 ")

    df_groupby['_col_list_joined'] = df_groupby['_col_list'].apply(lambda x: '#'.join(x).lower())

    print("\nGroupby aggregated dataFrame with lists and single occurrences removed:")
    print(df_groupby)

    df_merged = df.merge(df_groupby, on=group_column, how='left')

    df_merged = df_merged[df_merged['_col_list_joined'].str.contains(filter_string.lower(), na=False)]

    # df_merged['is_subset'] = set(df_merged['room']).issubset(set(df_merged['_col_list']))  # does not work:TypeError: unhashable type: 'list'

    df_merged['is_found_in_another'] = df_merged. \
        apply(lambda row: df_issubset(row[occurance_col], row['_col_list']), axis=1)

    print("\nMerged dataFrame with aggregated lists merged with repeats still in:")
    print(df_merged)

    df_merged = df_merged.loc[df_merged['is_found_in_another'] == True]
    print("\nMerged dataFrame with aggregated lists merged and non-repeat removed:")
    print(df_merged)

    # clean up temporary field
    df_merged = df_merged.drop(['is_found_in_another', '_col_list', '_row_count'], axis=1)

    return df_merged


def main():
    """ test out is_found_in_another function"""

    is_found = is_found_in_another(df, 'room', 'writer', 'HaaR')

    print("\nDataFrame: is_found_in_another")
    print(is_found)


if __name__ == '__main__':
    data = {
            'writer': ['jim', 'jim', 'kate', ],
            'room': ['NY', 'Haar', 'NY', ],
            'value_column': [10, 20, 30, ],
            }

    df = pd.DataFrame(data)
    del data
    print("DataFrame: df")
    print(df)

    main()
