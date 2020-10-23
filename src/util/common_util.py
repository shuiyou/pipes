import json
import traceback

import pandas as pd
from numpy import int64
from jsonpath import jsonpath

from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)


def to_string(obj):
    if obj is None:
        return ''
    return str(obj)


def format_timestamp(obj):
    if obj is not None:
        return obj.strftime('%Y-%m-%d')
    else:
        return ''


def exception(describe):
    def robust(actual_do):
        def add_robust(*args, **keyargs):
            try:
                return actual_do(*args, **keyargs)
            except Exception as e:
                logger.error(describe)
                logger.error(traceback.format_exc())

        return add_robust

    return robust


def replace_nan(values):
    v_list = [x if pd.notna(x) else 0 for x in values]
    result = []
    for v in v_list:
        if isinstance(v, int64):
            result.append(int(str(v)))
        else:
            result.append(v)

    return result


def get_query_data(msg, query_user_type, query_strategy):
    query_data_list = jsonpath(msg, '$..queryData[*]')
    resp = []
    for query_data in query_data_list:
        name = query_data.get("name")
        idno = query_data.get("idno")
        user_type = query_data.get("userType")
        strategy = query_data.get("strategy")
        if pd.notna(query_user_type) and user_type == query_user_type and strategy == query_strategy:
            resp_dict = {"name": name, "id_card_no": idno}
            resp.append(resp_dict)
        if pd.isna(query_user_type) and strategy == query_strategy:
            resp_dict = {"name": name, "id_card_no": idno}
            resp.append(resp_dict)
    return resp
