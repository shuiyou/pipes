# -*- coding: utf-8 -*-
import json
import re
from abc import ABCMeta, abstractmethod

import jsonpath
import numpy
import numpy as np
import pandas as pd


def numpy_to_int(variables):
    for key, value in variables.items():
        if type(value) == numpy.int64:
            variables[key] = int(value)


def extract_money(value):
    """
    行政违法记录模块从行政执法结果提取金额
    :param value:
    :return:
    """
    money = float('0.00')
    if value is not None and len(value) > 0:
        value = re.sub(r'\,', "", value)
        money_str = "0.00"
        pattern1 = re.compile(r'(?<=罚款金额\(单位：万元\)\:)\d+\.?\d*')
        pattern2 = re.compile(r'(?<=金额\:)\d+\.?\d*')
        pattern3 = re.compile(r'(?<=罚款)\d+\.?\d*')
        pattern4 = re.compile(r'(?<=罚款人民币)\d+\.?\d*')
        if pattern1.search(value) != None:
            money_str = pattern1.search(value).group(0)
        elif pattern2.search(value) != None:
            money_str = pattern2.search(value).group(0)
        elif pattern3.search(value) != None:
            money_str = pattern3.search(value).group(0)
        elif pattern4.search(value) != None:
            money_str = pattern4.search(value).group(0)
        money = float(money_str)
        if ("万元" in value):
            money = money * 10000
        money = float("%.2f" % money)
    return money


def extract_money_court_excute_public(value):
    """
    执行公开信息模块从执行内容中提取金额
    :param value:
    :return:
    """
    money_max = 0
    if value is not None and len(value) > 0:
        value = re.sub(r'\,', "", value)
        money_array = re.findall(r"\d+\.?\d*", value)
        if money_array is not None and len(money_array) > 0:
            for money in money_array:
                if ('万元') in value:
                    money_re = float(money) * 10000
                else:
                    money_re = float(money)
                if money_re > money_max:
                    money_max = money_re
            money_max = float("%.2f" % money_max)
    return money_max


def subtract_datetime_col(df, col_name1, col_name2, time_unit='M'):
    """
    计算两列时间差, 按天， 周，月或者年。
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


def parse_json_count_sum(data, expr):
    """
    解析json，获取json path里对应的值，然后求和
    :param data:
    :param expr:
    :return:
    """
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
        self.user_type = None
        self.variables = {}
        self.out_decision_code = {}

    def run(self, user_name=None, id_card_no=None, phone=None, user_type=None) -> dict:
        self.input(id_card_no, phone, user_name, user_type)
        self.transform()
        return {
            "variables": self.variables,
            "out_decision_code": self.out_decision_code
        }

    def input(self, id_card_no, phone, user_name, user_type=None):
        self.id_card_no = id_card_no
        self.user_name = user_name
        self.phone = phone
        self.user_type = user_type

    @abstractmethod
    def transform(self):
        """
        变量转换方法
        :return:
        """
        pass
