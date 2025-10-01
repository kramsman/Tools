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

    # filter_out = ['National-Bob Haar', 'National Bob Haar - Team Bob', 'ZZZZZZ National Bob Haar - Team Casey']

    if filter_out:  # filter rooms out from list
        for element in filter_out:
            df_func = df_func.loc[~(df_func['room'].map(standard_format) == standard_format(element))]

    if id_field is None:  # this is no group. need to set up simple case with list normally created via groupby
        id_field = check_field

    # Group by 'key_column' and aggregate 'value_column' into a list
    df_groupby = df_func.groupby(id_field) \
        .agg(_col_list=(check_field, list),
             _row_count=(check_field, 'count')
             ).reset_index()

    df_groupby['_col_list_joined'] = df_groupby['_col_list'].apply(lambda x: '#'.join(x).lower())

    print("\nGroupby aggregated dataFrame with lists and single occurrences removed:")
    print(df_groupby)

    df_merged = df_func.merge(df_groupby, on=id_field, how='left')

    df_merged['_set'] = df_merged['_col_list'].apply(lambda l: set(l),)
    df_merged['num_of_rooms'] = df_merged['_col_list'].apply(lambda l: len(set(l)),)  # TODO: is this the same as
    # row_count?

    if not single_row_incl:
        # keep single rows (having filter string). these have group_column in _col_list, which is always true,
        # but not really "is_found_in_another"
        df_merged = df_merged.query("_row_count > 1 ")  # occurrence always in list once; exclude where there is only one

    if one_room:  # one value occurring in multiple records
        df_merged = df_merged[df_merged['num_of_rooms'] == 1]
    else:
        df_merged = df_merged[df_merged['num_of_rooms'] != 1]

    # need to search string value of joined list (rather than use 'isin') so partial strings can be searched (isin
    # would not match)
    # df_merged = df_merged[df_merged['_col_list_joined'].str.contains(filter_string.lower(), na=False)]
    # df_merged['string_in_list'] = df_partial_in_list(filter_string.lower(), df_merged['_col_list'])
    # df_merged['is_subset'] = set(df_merged['room']).issubset(set(df_merged['_col_list']))  # does not work:TypeError: unhashable type: 'list'
    df_merged['string_in_list'] = df_merged['_col_list'].\
        apply(lambda x: df_partial_in_list(filter_string.lower(), x))

    df_merged['is_found_in_another'] = df_merged. \
        apply(lambda row: df_issubset(row[check_field], row['_col_list']), axis=1)

    # only the group we want
    df_merged = df_merged[df_merged['string_in_list']]

    df_merged = df_merged.loc[df_merged['is_found_in_another'] == True]
    print("\nMerged dataFrame with aggregated lists merged and non-repeat removed:")
    print(df_merged)

    # delete temporary fields
    # df_merged = df_merged.drop(['is_found_in_another', '_col_list', '_row_count'], axis=1)

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
