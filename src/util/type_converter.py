# @Time : 2020/10/23 5:45 PM 
# @Author : lixiaobo
# @File : type_converter.py 
# @Software: PyCharm
from numpy import int64
from pandas import Series
from pymysql import Timestamp

from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)


def echo_var_type(parent, key, val):
    try:
        if isinstance(val, list):
            t_list = []
            int64list = len(list(filter(lambda x: isinstance(x, int64), val))) > 0
            for v in val:
                if int64list:
                    if isinstance(v, int64):
                        t_list.append(int(str(v)))
                    else:
                        t_list.append(v)
                else:
                    echo_var_type(val, key, v)
            if int64list > 0:
                val.clear()
                for cv in t_list:
                    val.append(cv)

        elif isinstance(val, dict):
            for k in val:
                v = val.get(k)
                echo_var_type(val, k, v)
        elif isinstance(val, Timestamp):
            logger.warn(str(key) + "----------------Timestamp not support-------------------------" + str(val))
        elif isinstance(val, int64):
            if isinstance(parent, dict):
                parent[key] = int(str(val))
        elif isinstance(val, Series):
            logger.warn(str(key) + "----------------Series not support----------------" + str(val))
            parent[key] = val.to_list()
    except Exception as e:
        logger.error(str(e))
