# @Time : 2020/6/19 3:13 PM 
# @Author : lixiaobo
# @File : portrait_processor.py 
# @Software: PyCharm
from abc import abstractmethod


class PortraitProcessor(object):

    def __init__(self):
        self.variables = None
        self.user_name = None
        self.id_card_no = None
        self.origin_data = None
        self.cached_data = None

    def init(self, variables, user_name, id_card_no, origin_data, cached_data):
        self.variables = variables
        self.user_name = user_name
        self.id_card_no = id_card_no
        self.origin_data = origin_data
        self.cached_data = cached_data

    @abstractmethod
    def process(self):
        pass
