# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine

from config import GEARS_DB, app_env

if app_env == 'dev':
    DB_URI = 'mysql+pymysql://%(user)s:%(pw)s@%(host)s:%(port)s/%(db)s' % GEARS_DB
else:
    DB_URI = 'mysql+pymysql://%(user)s:%(pw)s@%(host)s:%(port)s/%(db)s' % GEARS_TEST_DB

__DB_ENGINE = create_engine(DB_URI, encoding="utf8")


def sql_to_df(sql, index_col=None, coerce_float=True, params=None,
              parse_dates=None, columns=None, chunksize=None):
    connect = __DB_ENGINE.connect()
    df = pd.read_sql(sql, con=connect, index_col=index_col, coerce_float=coerce_float, params=params,
                     parse_dates=parse_dates, columns=columns, chunksize=chunksize)
    connect.close()
    return df


def sql_insert(sql, index_col=None, coerce_float=True, params=None,
               parse_dates=None, columns=None, chunksize=None):
    connect = __DB_ENGINE.connect()
    connect.execute(sql)
    connect.close()
