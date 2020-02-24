def df_compare(variables, old_df, new_df, variable_name):
    if (old_df.shape[0] == 0 and new_df.shape[0] == 0) or new_df.shape[0] == 0:
        return

    find = True
    for index, row in new_df.iterrows():
        find &= find_value(old_df, row[0])

    variables[variable_name] = 0 if find else 1


def find_value(df, val):
    for index, row, in df.iterrows():
        if val == row[0]:
            return True
    return False
