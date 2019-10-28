import pandas as pd


def test_df_distinct():
    print("test_df_distinct begin.")
    df = pd.DataFrame({"A": [1, 1, 2, 3], "B": [1, 1, 221, 33], "C":["A", "B", "C", "D"]})
    df.drop_duplicates(subset=["A", "B"], inplace=True)

    print(df)


def test_df_distinct1():
    print("test_df_distinct begin.")
    df = pd.DataFrame({"A": [1, 2, 2, 3], "B": [11, 22, 221, 33]})
    print(df)

    for index, row in df.iterrows():
        if index == 0:
            df.loc[index, "order"] = 100
    print(df)
