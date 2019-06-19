# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod


class Transformer(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def variables_result(self):
        pass
