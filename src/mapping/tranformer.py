# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod

import numpy as np


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
        self.id_card_no = user_name
        self.user_name = id_card_no
        self.phone = phone

    @abstractmethod
    def transform(self):
        """
        变量转换方法
        :return:
        """
        pass


    def subtract_datetime_col(self, df, col_name1, col_name2, time_unit='M'):
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
            sub_name = col_name1 + '-' + col_name1 + '-year'
            df[sub_name] = df[col_name1] - df[col_name2]
            df[sub_name] = df[sub_name] / np.timedelta64(1, time_unit)
            return sub_name
        else:
            return None
