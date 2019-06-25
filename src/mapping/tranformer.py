# -*- coding: utf-8 -*-
import json

import jsonpath
import pandas as pd
from abc import ABCMeta, abstractmethod

import numpy as np


def subtract_datetime_col(df, col_name1, col_name2, time_unit='M'):
    """
    计算两列时间差
    :param df:
    :param col_name1:
    :param col_name2:
    :param time_unit: 时间差的单位 D 天, W 周, M 月, Y 年
    :return:
    """
    cols = df.columns
    if col_name1 in cols and col_name2 in cols:
        sub_name = col_name1 + '_' + col_name2 + time_unit
        df[col_name1] = pd.to_datetime(df[col_name1])
        df[col_name2] = pd.to_datetime(df[col_name2])
        df[sub_name] = df[col_name1] - df[col_name2]
        df[sub_name] = df[sub_name] / np.timedelta64(1, time_unit)
        return sub_name
    else:
        return None


def parse_json_count(data, expr):
    json_path_obj = jsonpath.jsonpath(json.loads(data), expr)
    result = 0
    for c in json_path_obj:
        result += int(c)
    return result


class Transformer(object):
    __metaclass__ = ABCMeta

    def __init__(self) -> None:
        super().__init__()
        self.id_card_no = None
        self.user_name = None
        self.phone = None
        self.variables = {}

    def run(self, user_name=None, id_card_no=None, phone=None) -> dict:
        self.input(id_card_no, phone, user_name)
        self.transform()
        return self.variables

    def input(self, id_card_no, phone, user_name):
        self.id_card_no = id_card_no
        self.user_name = user_name
        self.phone = phone

    @abstractmethod
    def transform(self):
        """
        变量转换方法
        :return:
        """
        pass
