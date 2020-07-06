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
        return temp_date.date()
    else:
        return datetime.datetime(temp_date.year, temp_date.month, end_day).date()


def transform_class_str(params, class_name):
    func_str = class_name + '('
    for k, v in params.items():
        if v is not None and v != '':
            func_str += k + "='" + str(v) + "',"
    func_str = func_str[:-1]
    func_str += ')'
    value = eval(func_str)
    return value


class TransFlow(ModuleProcessor, ABC):

    def __init__(self):
        super().__init__()
        # self.trans_flow_portrait_df = self._time_interval('trans_flow_portrait')
        # self.trans_flow_portrait_df_2_years = self._time_interval('trans_flow_portrait', year=2)
        self.db = self._db()
        self.variables = {}

    def _time_interval(self, tablename, year=1):
        flow_df = self.cached_data(tablename)
        flow_df['trans_date'] = pd.to_datetime(flow_df['trans_date'])
        max_date = max(flow_df['trans_date'])
        min_date = min(flow_df['trans_date'])

        if year != 1:
            if max_date.month == 12:
                years_before_first = datetime.datetime(max_date.year - year + 1, 1, 1)
            else:
                years_before_first = datetime.datetime(max_date.year - year, max_date.month + 1, 1)
        else:
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
