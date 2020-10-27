# @Time : 2020/10/21 10:29 AM 
# @Author : lixiaobo
# @File : np_encoder.py 
# @Software: PyCharm
import json

from numpy import integer, floating, ndarray


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, integer):
            return int(obj)
        elif isinstance(obj, floating):
            return float(obj)
        elif isinstance(obj, ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)
