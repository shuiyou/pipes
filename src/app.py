import importlib
import json
import time

from flask import Flask, request, jsonify
from py_eureka_client import eureka_client
from werkzeug.exceptions import HTTPException

from config import EUREKA_SERVER, version_info
from config_controller import base_type_api
from exceptions import APIException, ServerException
from logger.logger_util import LoggerUtil
from parser.Parser import Parser
from product.generate import Generate
from util.defensor_client import DefensorClient

logger = LoggerUtil().logger(__name__)

app = Flask(__name__)
app.register_blueprint(base_type_api)
start_time = time.localtime()


logger.info("init eureka client...")
logger.info("EUREKA_SERVER:%s", EUREKA_SERVER)
eureka_client.init(eureka_server=EUREKA_SERVER,
                   app_name="PIPES",
                   instance_port=5000)
logger.info("eureka client started. center: %s", EUREKA_SERVER)


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
    logger.info("strategy begin...")
    json_data = request.get_json()
    logger.info("strategy param:%s", json_data)
    strategy_param = json_data.get('strategyParam')
    product_code = strategy_param.get('productCode')
    handler = _get_product_handler(product_code)
    df_client = DefensorClient(request.headers)
    handler.df_client = df_client

    resp = handler.call_strategy(json_data)
    return jsonify(resp)


@app.route("/parse", methods=['POST'])
def parse():
    """
    流水解析，验真请求
    """
    file = request.files.get("file")
    function_code = request.args.get("parseCode")
    if function_code is None:
        function_code = request.form.get("parseCode")
    data = request.args.get("param")
    if data is None:
        data = request.form.get("param")

    if function_code is None:
        return "缺少 parseCode字段"
    elif data is None:
        return "缺少 param字段"
    elif file is None:
        return "缺少 file字段"

    handler = _get_handler("parser", "Parser", function_code)
    handler.init_param(json.loads(data), file)
    resp = handler.process()

    return jsonify(resp)


@app.route("/health", methods=['GET'])
def health_check():
    """
    检查当前应用的健康情况
    :return:
    """
    return 'pipes is running'


@app.route("/info", methods=['GET'])
def info():
    return 'pipes is running'


# 获取系统基本参数信息，用于系统监控
@app.route("/sys-basic-info", methods=['GET'])
def sys_basic_info():
    return jsonify({
        "SysName": "Pipes",
        "Version": version_info,
        "StartTime": time.strftime("%Y-%m-%d %H:%M:%S", start_time)
    })


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


def _get_product_handler(product_code) -> Generate:
    try:
        model = importlib.import_module("product.p" + str(product_code))
        api_class = getattr(model, "P" + str(product_code))
        api_instance = api_class()
        return api_instance
    except ModuleNotFoundError as err:
        logger.error(str(err))
        return Generate()


def _get_handler(folder, prefix, code) -> Parser:
    try:
        model = importlib.import_module(folder + "." + prefix + str(code))
        api_class = getattr(model, prefix + str(code))
        api_instance = api_class()
        return api_instance
    except ModuleNotFoundError as err:
        logger.error(str(err))
        return Parser()


if __name__ == '__main__':
    logger.info('starting pipes...')
    app.run(host='0.0.0.0')
    logger.info('pipes started.')
