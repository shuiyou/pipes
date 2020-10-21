# @Time : 2020/10/21 10:29 AM 
# @Author : lixiaobo
# @File : np_encoder.py 
# @Software: PyCharm
import json

from pandas.tests.extension.numpy_.test_numpy_nested import np


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)