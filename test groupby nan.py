""" see if dropna=false option works"""

import pandas as pd
def groupby_nan(df):
    dfgrp = df.groupby('room', dropna=False).sum()
    return dfgrp


data = {
        'writer': ['jim', 'jim', 'kate', 'kate'],
        'room': ['NY', 'NY', 'CA', None],
        'value_column': [10, 20, 30, 40],
        }

df = pd.DataFrame(data)
# del data
print("DataFrame: df")
print(df)

xx = groupby_nan(df)

a=1
