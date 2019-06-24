# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod


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
