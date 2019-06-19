# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod


class Transformer(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def transform(self):
        """
        变量转换方法
        :return:
        """
        pass

    @abstractmethod
    def variables_result(self):
        """获取转换后的结果"""
        pass

    @abstractmethod
    def get_biz_type(self):
        """
        获取对应的业务类型
        :return:
        """
        pass
