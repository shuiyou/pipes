import importlib

from flask import Flask, request, jsonify
from py_eureka_client import eureka_client
from werkzeug.exceptions import HTTPException

from config import EUREKA_SERVER
from exceptions import APIException, ServerException
from logger.logger_util import LoggerUtil
from product.generate import Generate
from util.defensor_client import DefensorClient

logger = LoggerUtil().logger(__name__)

app = Flask(__name__)


@app.route("/biz-types", methods=['POST'])
def shake_hand():
    """
    根据productCode调用对应的handler处理业务
    :return:
    """
    json_data = request.get_json()
    product_code = json_data.get('productCode')
    handler = _get_product_handler(product_code)
    df_client = DefensorClient(request.headers)
    handler.df_client = df_client

    resp = handler.shake_hand(json_data)
    logger.info("shake_hand------end-------")
    return jsonify(resp)


@app.route("/strategy", methods=['POST'])
def strategy():
    json_data = request.get_json()
    strategy_param = json_data.get('strategyParam')
    product_code = strategy_param.get('productCode')
    handler = _get_product_handler(product_code)
    df_client = DefensorClient(request.headers)
    handler.df_client = df_client

    resp = handler.call_strategy(json_data)
    return jsonify(resp)


def _get_product_handler(product_code) -> Generate:
    try:
        model = importlib.import_module("product.p" + str(product_code))
        api_class = getattr(model, "P" + str(product_code))
        api_instance = api_class()
        return api_instance
    except ModuleNotFoundError as err:
        logger.error(str(err))
        return Generate()


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
    if isinstance(e, HTTPException):
        error = APIException()
        error.code = e.code
        error.description = e.description
        return error
    # 异常肯定是Exception
    from flask import current_app
    # 如果是调试模式,则返回e的具体异常信息。否则返回json格式的ServerException对象！
    if current_app.config["DEBUG"]:
        return e
    return ServerException()


def _init_eureka_client():
    logger.info("EUREKA_SERVER:%s", EUREKA_SERVER)
    eureka_client.init(eureka_server=EUREKA_SERVER,
                       app_name="PIPES",
                       instance_port=8010)


if __name__ == '__main__':
    logger.info("init eureka client...")
    _init_eureka_client()
    logger.info('starting pipes...')
    app.run(host='0.0.0.0')
    logger.info('pipes started.')
