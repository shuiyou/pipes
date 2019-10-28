import pandas as pd


def test_df_distinct():
    print("test_df_distinct begin.")
    df = pd.DataFrame({"A": [1, 2, 2, 3], "B": [11, 22, 221, 33]})
    print(df)
    df["order"] = 0

    for index, row in df.iterrows():
        if row["A"] == 1:
            row["order"] = 1
        else:
            row["order"] = 100

    print(df)


def test_df_distinct1():
    print("test_df_distinct begin.")
    df = pd.DataFrame({"A": [1, 2, 2, 3], "B": [11, 22, 221, 33]})
    print(df)

    for index, row in df.iterrows():
        if index == 0:
            df.loc[index, "order"] = 100


    print(df)
