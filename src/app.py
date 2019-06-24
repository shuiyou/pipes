import os
import sys

import requests
from flask import Flask, request, jsonify
from jsonpath import jsonpath
from werkzeug.exceptions import HTTPException

from config import STRATEGY_URL
from exceptions import APIException, ServerException
from logger.logger_util import LoggerUtil
from mapping.mapper import translate

logger = LoggerUtil().logger(__name__)

file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)

app = Flask(__name__)


def _get_process_code(product_code):
    """
    根据产品编码得到对应的process code： 决策引擎的流程编码
    :param product_code:
    :return:
    """
    # TODO 需要配置一下product_code 和 决策的process code映射表
    return 'Level1_m'


def _build_request(req_no, product_code, variables={}):
    """
    根据http请求构建出决策需要的请求, 需要去查数据库获取相关的数据
    :return:
    """
    strategy_request = {
        "StrategyOneRequest": {
            "Header": {
                "InquiryCode": req_no,
                "ProcessCode": _get_process_code(product_code)
            },
            "Body": {
                "Application": {
                    "Variables": variables
                }
            }
        }
    }
    return strategy_request


@app.route("/biz-types", methods=['POST'])
def shake_hand():
    """
    根据产品编码获取该产品对应的业务类型
    :return:
    """
    # 获取请求参数
    json_data = request.get_json()
    req_no = json_data.get('reqNo')
    product_code = json_data.get('productCode')
    strategy_request = _build_request(req_no, product_code)
    # 调用决策引擎
    response = requests.post(STRATEGY_URL, json=strategy_request)
    if response.status_code == 200:
        json = response.json()
        resp = {
            'productCode': json_data.get('productCode'),
            'reqNo': json_data.get('reqNo'),
            'bizTypes': _get_biz_types(json)
        }
        return jsonify(resp)
    else:
        raise ServerException(code=response.status_code, description=response.text)


def _get_biz_types(json):
    res = jsonpath(json, '$..out_strategyBranch')
    if isinstance(res, list) and len(res) > 0:
        biz_types = res[0].split(',')
    else:
        biz_types = []
    return biz_types


@app.route("/strategy", methods=['POST'])
def strategy():
    """
    决策调用，然后返回结果
    输入参数是一个json对象
    :return:
    """
    # 获取请求参数
    json_data = request.get_json()
    req_no = json_data.get('reqNo')
    product_code = json_data.get('productCode')
    user_name = json_data.get('userName')
    id_card_no = json_data.get('idCardNo')
    phone = json_data.get('phone')
    codes = json_data.get('bizTypes')
    variables = translate(codes, user_name, id_card_no, phone)
    strategy_request = _build_request(req_no, product_code, variables)
    logger.debug(strategy_request)
    # 调用决策引擎
    strategy_response = requests.post(STRATEGY_URL, json=strategy_request)
    if strategy_response.status_code == 200:
        json = strategy_response.json()
        resp = {
            'reqNo': req_no,
            'bizTypes': _get_biz_types(json),
            'data': json
        }
        return jsonify(resp)
    else:
        raise ServerException(code=strategy_response.status_code, description=strategy_response.text)


@app.route("/health", methods=['GET'])
def health_check():
    """
    检查当前应用的健康情况
    :return:
    """
    return 'pipes is running'


@app.errorhandler(Exception)
def flask_global_exception_handler(e):
    # 判断异常是不是APIException
    if isinstance(e, APIException):
        return e
    # 判断异常是不是HTTPException
    elif isinstance(e, HTTPException):
        error = APIException()
        error.code = e.code
        error.description = e.description
        return error
    # 异常肯定是Exception
    else:
        from flask import current_app
        # 如果是调试模式,则返回e的具体异常信息。否则返回json格式的ServerException对象！
        if current_app.config["DEBUG"]:
            return e
        else:
            return ServerException()


if __name__ == '__main__':
    app.run(host='0.0.0.0')
