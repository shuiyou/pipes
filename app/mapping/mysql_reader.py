import pandas as pd
from sqlalchemy import create_engine

from config import GEARS_DB

DB_URI = 'mysql+pymysql://%(user)s:%(pw)s@%(host)s:%(port)s/%(db)s' % GEARS_DB

__DB_ENGINE = create_engine(DB_URI, encoding="utf8")


def sql_to_df(sql, index_col=None, coerce_float=True, params=None,
              parse_dates=None, columns=None, chunksize=None):
    connect = __DB_ENGINE.connect()
    df = pd.read_sql(sql, con=connect, index_col=None, coerce_float=True, params=None,
                       parse_dates=None, columns=None, chunksize=None)
    connect.close()
    return df
