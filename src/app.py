import uuid

import requests
from flask import Flask, request, jsonify
from jsonpath import jsonpath
from werkzeug.exceptions import HTTPException

from config import STRATEGY_URL
from exceptions import APIException, ServerException
from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)

app = Flask(__name__)


def _build_request(input):
    """
    根据http请求构建出决策需要的请求, 需要去查数据库获取相关的数据
    :return:
    """
    # TODO: 实现请求，数据映射的代码请写在mapping包里
    strategy_request = {
        "StrategyOneRequest": {
            "Header": {
                "InquiryCode": str(uuid.uuid4()),
                "ProcessCode": input.get('productCode')
            },
            "Body": {
                "Application": {
                    "Variables": input.get('queryData') if input.get('queryData') is not None else {}
                }
            }
        }
    }
    return strategy_request


def build_response(json):
    return json


@app.route("/shake-hand", methods=['GET'])
def shake_hand():
    # 获取请求参数
    json_data = request.get_json()
    strategy_request = _build_request(json_data)
    # 调用决策引擎
    response = requests.post(STRATEGY_URL, json=strategy_request)
    json = response.json()
    res = jsonpath(json, '$..out_strategyBranch')
    return jsonify(res)


@app.route("/dispatch", methods=['POST'])
def dispatch():
    """
    应用的统一入口，获取数据分发给不通的数据映射和决策，然后返回结果
    :return:
    """
    # 获取请求参数
    json_data = request.get_json()

    # TODO: 实现dispatcher, 根据json_data的指令去获取数据，做对应的数据处理，然后调用对应的决策
    strategy_request = _build_request(json_data)
    logger.debug(strategy_request)
    # 调用决策引擎
    strategy_response = requests.post(STRATEGY_URL, json=strategy_request)

    # TODO：需要转换成约定好的输出schema形势
    return jsonify(build_response(strategy_response.json()))


@app.route("/health", methods=['GET'])
def health_check():
    return 'pipes is up'


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
