import pandas as pd

from util.mysql_reader import sql_to_df


def df_compare(variables, old_df, new_df, variable_name):
    if (old_df.shape[0] == 0 and new_df.shape[0] == 0) or new_df.shape[0] == 0:
        return

    find = True
    for index, row in new_df.iterrows():
        find &= _find_value(old_df, row[0])

    variables[variable_name] = 0 if find else 1


def _find_value(df, val):
    for index, row, in df.iterrows():
        if val == row[0]:
            return True
    return False


def sql_list_to_df(transformer, sql_arr, param):
    params = {"user_name": transformer.user_name,
              "id_card_no": transformer.id_card_no,
              "pre_biz_date": transformer.pre_biz_date}
    for key in param:
        params[key] = param[key]
    df = pd.DataFrame()
    for sql in sql_arr:
        v_df = sql_to_df(sql=sql,
                         params=params)
        if df.shape[0] == 0 and v_df.shape[0] > 0:
            df = v_df
        elif v_df.shape[0] > 0:
            df.append(v_df)
    return df


def var_compare(transformer, new_sql, old_sql, variable_name, extra_param={}):
    params = {"user_name": transformer.user_name,
              "id_card_no": transformer.id_card_no,
              "pre_biz_date": transformer.pre_biz_date}

    for k in extra_param:
        params[k] = extra_param[k]
    old_df = sql_to_df(sql=old_sql,
                       params=params)
    new_df = sql_to_df(sql=new_sql,
                       params=params)
    df_compare(transformer.variables, old_df, new_df, variable_name)
