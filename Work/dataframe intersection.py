""" return records matched by common field in first df but not second"""

import pandas as pd

def a_not_in_b(left, right, match_field):

    merged = (left.merge(right, on=match_field, how='left', indicator=True)
     .query('_merge == "left_only"')
     .drop('_merge', axis=1))

    merged = merged[left.columns]

    # left.merge(right, on=match_field, how='left', indicator=True)

    return merged


def main_program():

    df1 = pd.DataFrame({'key1': ['K0', 'K0', 'K1', 'K2'],
                        'A': ['A0', 'A1', 'A2', 'A3'],
                        'B': ['B0', 'B1', 'B2', 'B3']})
    print(df1)

    df2 = pd.DataFrame({'key1': ['K1', 'K1', 'K2', 'K3'],
                        'C': ['C0', 'C1', 'C2', 'C3'],
                        'D': ['D0', 'D1', 'D2', 'D3']})

    print(df2)

    merged = a_not_in_b(df1, df2, 'key1')
    print(merged)

    a=1

if __name__ == '__main__':
    main_program()
