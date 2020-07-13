import pandas as pd


def df_value(series):
    if series.empty:
        return 0
    else:
        return series.iloc[0]


def nan_to_zero(val):
    if pd.isna(val):
        return 0
    else:
        return val
