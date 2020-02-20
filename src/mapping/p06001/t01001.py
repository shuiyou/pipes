# -*- coding: utf-8 -*-
# @Time : 2020/2/20 1:45 PM
# @Author : lixiaobo
# @Site : 
# @File : t01001.py
# @Software: PyCharm
from mapping.tranformer import Transformer


class T01001(Transformer):
    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'variable_test': 'test'
        }

    def transform(self):
        self.variables["variable_test"] = "test1"
