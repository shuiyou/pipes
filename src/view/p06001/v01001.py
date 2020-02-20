# -*- coding: utf-8 -*-
# @Time : 2020/2/20 1:45 PM
# @Author : lixiaobo
# @Site :
# @File : t01001.py
# @Software: PyCharm
from mapping.tranformer import Transformer


class V17001(Transformer):
    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'view_variable_test': 0,
        }

    def transform(self):
        self.variables["view_variable_test"] = 1
