from abc import ABC

from mapping.module_processor import ModuleProcessor
import datetime
import pandas as pd
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from config import GEARS_DB


def months_ago(end_date, months):
    end_year = end_date.year
    end_month = end_date.month
    end_day = end_date.day
    if end_month < months:
        res_month = 12 + end_month - months + 1
        res_year = end_year - 1
    else:
        res_month = end_month - months + 1
        res_year = end_year
    temp_date = datetime.datetime(res_year, res_month, 1) - datetime.timedelta(days=1)
    if temp_date.day <= end_day:
        return temp_date
    else:
        return datetime.datetime(temp_date.year, temp_date.month, end_day)


def transform_enumerate(params, key_str, mapping, non_str):
    if params.__contains__(key_str):
        if mapping.__contains__(params[key_str]):
            params[key_str] = mapping[params[key_str]]
        else:
            params[key_str] = non_str


def transform_dict(detailinfo, mapping):
    params = dict()
    for k, v in mapping.items():
        if type(v) == list:
            temp = detailinfo
            for _ in v:
                if temp.__contains__(_):
                    temp = temp[_]
                else:
                    temp = ''
                    break
            params[k] = temp
        else:
            params[k] = v
    return params


class TransModuleProcessor(ModuleProcessor, ABC):

    def __init__(self):
        super().__init__()
        self.trans_flow_portrait_df = self._time_interval('trans_flow_portrait')
        self.db = self._db()

    def _time_interval(self, tablename, year=1):
        flow_df = self.cached_data(tablename)
        flow_df['trans_date'] = pd.to_datetime(flow_df['trans_date'])
        max_date = max(flow_df['trans_date'])
        min_date = min(flow_df['trans_date'])

        years_before_first = datetime.datetime(max_date.year - year, max_date.month, 1)
        min_date = min(min_date, years_before_first)
        flow_df = flow_df[(flow_df.trans_date >= min_date) &
                          (flow_df.trans_date <= max_date)]
        return flow_df

    @staticmethod
    def _db():
        app = Flask(__name__)

        db_url = 'mysql+pymysql://%(user)s:%(pw)s@%(host)s:%(port)s/%(db)s' % GEARS_DB
        app.config['SQLALCHEMY_DATABASE_URI'] = db_url
        app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        app.config['SQLALCHEMY_ECHO'] = True
        db = SQLAlchemy(app)
        return db
