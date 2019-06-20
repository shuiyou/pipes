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

    @abstractmethod
    def transform(self, user_name=None, id_card_no=None, phone=None):
        """
        变量转换方法
        :return:
        """
        pass


