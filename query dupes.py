""" return records matched by common field in first df but not second"""

import pandas as pd


def df_issubset(item, list_from_df_element):
    """ issubset function when list is an element of a dataframe
    (I think doesn't work because list is returned as series"""

    # import math
    # normal_list = [x for x in list_from_df_element]
    normal_list = list_from_df_element
    if not isinstance(item, list):
        item_list = [item]
    else:
        item_list = item

    if not isinstance(normal_list, list):
    # if math.isnan(normal_list):
        is_subset = False
    else:
        is_subset = set(item_list).issubset(normal_list)
    # is_subset = set(item_list).issubset(set(normal_list))

    return is_subset

def is_found(df, occurance_col, group_column=None):  # TODO add filter field, found in list or not
    """ returns records with col_name value found in other records
    """

    # def myfunc(list_from_df_element):
    #     simple_list = [x for x in list_from_df_element]
    #     return simple_list

    # TODO explore isin instead of issubset

    if group_column is None:  # need to set up simple case with list normally created via groupby
        # df['_occurance_col_list'] = df[occurance_col]
        group_column = occurance_col

    # Group by 'key_column' and aggregate 'value_column' into a list
    df_groupby = df.groupby(group_column) \
        .agg(_col_list=(occurance_col, list),
             _row_count=(occurance_col, 'count')
             ).reset_index() \
        .query("_row_count > 1 ")

    # df_agg_as_list = df.groupby('group_column')['occurance_col'].\
    #     agg([list, 'count'] ).reset_index().\
    #     query("count > 1 ")

    # Rename the aggregated column for clarity (optional)
    # df_groupby.rename(columns={'occurance_col': '_occurance_col_list'}, inplace=True)

    print("\nGroupby aggregated dataFrame with lists and single occurrences removed:")
    print(df_groupby)

    # you can use pd.merge() or df.merge()
    df_merged = df.merge(df_groupby, on=group_column, how='left')

    # df_merged['is_subset'] = set(occurance_col).issubset('_col_list')

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



    # a=1

    # df['list_by_comprehension'] = [element for element in df['list_column']]
    # df['inlist'] = set(df['list_column'])  # FAILED: TypeError: unhashable type: 'list'

    # df['list_by_lambda_and_comprehension'] = df['list_column'].apply(
    #     lambda passed_list: [element for element in df['list_column']])
    # df['list_by_lambda_and_myfunc_comprehension'] = df["other_column"].issubset(df['list_column']. \
    #                                                     apply(lambda passed_list: myfunc(passed_list)))

    # df['list_by_lambda_and_myfunc2'] = df[['other_column','list_column']].\
    #     apply(lambda row: df_issubset(row['other_column'], row['list_column']))

    # print("DataFrame: df")
    # print(df)

    # x_list = df_merged[df_merged['X'].isin(df_merged['other_list'])]
    # df_merged['inlist'] = df_merged['other_column'].isin(df_merged['other_list'])
    # x_list = df_merged["other_list"].isin(["element"])
    # print(df_merged["other_list"].isin(["element"]))
    # df.isin({"name": ["Sally"], "age": [30]}))

    # x_list2 = df_merged.loc[
    #     (df_merged['key_column'].str.contains('element', case=False, na=False))]

    # return df


def main_program():

    # data = {
    #         'value_column': [10, 20, 30, 40,],
    #         'key_column': ['A', 'B', 'A', 'C',],
    #         'other_column': [['x'], ['x'], ['z'], ['w'],],
    #         'list_column': [('x','y'), ('x'), ('z'), ('w'),],
    #         }

    data = {
            'writer': ['jim', 'jim', 'kate',],
            'room': ['NY', 'CA', 'NY',],
            'value_column': [10, 20, 30,],
            }

    # data = {
    #         'other_column': ['x', 'y',],
    #         'list_column': [['x','y'], ['x'],],
    #         }

    df = pd.DataFrame(data)
    print("DataFrame: df")
    print(df)

    # df_agg_as_list = df.groupby('writer')['room'].agg([list, 'count'] ).reset_index().\
    #     query("count > 1 ")

    # with rename
    # df_agg_as_list = df.groupby('writer')\
    #     .agg(_col_list=('room', list),
    #     _row_count=('room', 'count')
    #          ).reset_index()\
    #     .query("_row_count > 1 ")
    #
    # df_merged = df.merge(df_agg_as_list, on='writer', how='left')
    # # df_merged['list'] = df['list'].fillna('nan')
    #
    # df_merged['inlist'] = df_merged.\
    #     apply(lambda row: df_issubset(row['room'], row['_col_list']), axis=1)


    # df['common'] = is_found(df, 'writer', 'room',)
    # sets column 'is_found_' if record's room is found in another record
    is_found_in_another = is_found(df, 'room', )
    # is_found_in_another = is_found(df, 'room', 'writer', )

    print("\nDataFrame: is_found_in_another")
    print(is_found_in_another)

    # df['inlist'] = df["other_column"].isin(["element", "y"])
    # df['inlist'] = pd.DataFrame(df['list_column'].apply(lambda element: element))
    #  df1 = df.apply(lambda element: element * element)
    # df['inlist'] = df["other_column"].isin(df['list_column'].apply(lambda element: element.copy()))
    # df['inlist'] = set(df["other_column"]).issubset(set(df['list_column']))
    # df['inlist'] = [set(element).issubset(df["other_column"].tolist()) for element in df['list_column']]
    # df['inlist'] = df["other_column"].issubset(set([element for element in df['list_column']]))

    a=1

if __name__ == '__main__':
    main_program()
