# @Time : 2020/6/19 3:13 PM 
# @Author : lixiaobo
# @File : portrait_processor.py 
# @Software: PyCharm
from abc import abstractmethod


class PortraitProcessor(object):

    def __init__(self):
        self.variables = None
        self.user_name = None
        self.user_type = None
        self.base_type = None
        self.id_card_no = None
        self.phone = None
        self.bankName = None
        self.bankAccount = None
        self.origin_data = None
        self.cached_data = None

    def init(self, variables, user_name, user_type, base_type, id_card_no, phone, bank_name, bank_account, origin_data, cached_data):
        self.variables = variables
        self.user_name = user_name
        self.user_type = user_type
        self.base_type = base_type
        self.id_card_no = id_card_no
        self.phone = phone
        self.bankName = bank_name
        self.bankAccount = bank_account
        self.origin_data = origin_data
        self.cached_data = cached_data

    @abstractmethod
    def process(self):
        pass
