# @Time : 2020/10/21 10:29 AM 
# @Author : lixiaobo
# @File : np_encoder.py 
# @Software: PyCharm
import json

from numpy import integer, floating, ndarray
from pandas import Series
from pymysql import Timestamp


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, integer):
            return int(obj)
        elif isinstance(obj, floating):
            return float(obj)
        elif isinstance(obj, ndarray):
            return obj.tolist()
        elif isinstance(obj, Series):
            return obj.to_list()
        elif obj and isinstance(obj, Timestamp):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return super(NpEncoder, self).default(obj)
