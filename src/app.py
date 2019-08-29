import json

import numpy
import requests
from flask import Flask, request, jsonify
from jsonpath import jsonpath
from werkzeug.exceptions import HTTPException

from config import STRATEGY_URL, product_code_process_dict
from exceptions import APIException, ServerException
from logger.logger_util import LoggerUtil
from mapping.mapper import translate_for_strategy
from mapping.t00000 import T00000
from view.mapper_detail import translate_for_report_detail, STRATEGE_DONE

logger = LoggerUtil().logger(__name__)

app = Flask(__name__)


def _append_rules(biz_types):
    rules = [
        {
            "code": "f0003",
            "data": [
                {
                    "relatedType": "SHAREHOLDER",
                    "rule": {
                        "entStatus": "OPENING",
                        "ratioOfInvestments": "0.2"
                    }
                },
                {
                    "relatedType": "LEGAL",
                    "rule": {
                        "entStatus": "OPENING"
                    }
                }
            ]
        }
    ]
    result = []
    for r in rules:
        if r['code'] in biz_types:
            result.append(r)
    return result


def _get_process_code(product_code):
    """
    根据产品编码得到对应的process code： 决策引擎的流程编码
    :param product_code:
    :return:
    """
    if product_code in product_code_process_dict.keys():
        return product_code_process_dict.get(product_code)
    raise Exception("产品编码：{} 不能找到对应的决策流程".format(product_code))


def _build_request(req_no, product_code, variables=None):
    """
    根据http请求构建出决策需要的请求, 需要去查数据库获取相关的数据
    :return:
    """
    # 替換None值，因爲決策不認
    for key, value in variables.items():
        if value is None:
            variables[key] = ''
        if type(value) is numpy.float64:
            variables[key] = round(value, 2)
        if str(value) == 'nan':
            variables[key] = 0

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
    try:
        json_data = request.get_json()
        logger.debug(json.dumps(json_data))
        req_no = json_data.get('reqNo')
        product_code = json_data.get('productCode')
        query_data = json_data.get('queryData')
        user_name = query_data.get('name')
        id_card_no = query_data.get('idno')
        phone = query_data.get('phone')
        user_type = query_data.get('userType')
        variables = T00000().run(user_name, id_card_no, phone, user_type)['variables']
        # 决策要求一直要加上00000，用户基础信息。
        variables['out_strategyBranch'] = '00000'
        strategy_request = _build_request(req_no, product_code, variables=variables)
        logger.info(strategy_request)
        # 调用决策引擎
        response = requests.post(STRATEGY_URL, json=strategy_request)
        if response.status_code != 200:
            raise Exception("strategyOne错误:" + response.text)
        resp_json = response.json()
        error = jsonpath(resp_json, '$..Error')
        if error:
            raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(resp_json, '$..Description')))
        biz_types = _get_biz_types(resp_json)
        resp = {
            'productCode': json_data.get('productCode'),
            'reqNo': json_data.get('reqNo'),
            'bizType': biz_types,
            'rules': _append_rules(biz_types)
        }
        return jsonify(resp)
    except Exception as err:
        logger.error(str(err))
        raise ServerException(code=500, description=str(err))


def _get_biz_types(input_json):
    res = jsonpath(input_json, '$..out_strategyBranch')
    if isinstance(res, list) and len(res) > 0:
        biz_types = res[0].split(',')
    else:
        biz_types = []
    return biz_types


def _relation_risk_subject(strategy_resp, out_decision_code):
    branch_code = jsonpath(strategy_resp, '$.StrategyOneResponse.Body.Application.Categories..Variables')
    if type(branch_code) == bool:
        return

    for c in branch_code:
        code_key = c['out_decisionBranchCode']
        if code_key is not None and code_key in out_decision_code.keys():
            c['queryData'] = out_decision_code[code_key]


@app.route("/strategy", methods=['POST'])
def strategy():
    """
    决策调用，然后返回结果
    输入参数是一个json对象
    :return:
    """
    try:
        # 获取请求参数
        json_data = request.get_json()
        logger.debug(json.dumps(json_data))
        strategy_param = json_data.get('strategyParam')
        origin_input = json_data.get('strategyInputVariables')
        if origin_input is None:
            origin_input = {}
        req_no = strategy_param.get('reqNo')
        product_code = strategy_param.get('productCode')
        query_data = strategy_param.get('queryData')
        user_name = query_data.get('name')
        id_card_no = query_data.get('idno')
        phone = query_data.get('phone')
        user_type = query_data.get('userType')
        codes = strategy_param.get('bizType')
        biz_types = codes.copy()
        biz_types.append('00000')
        variables, out_decision_code = translate_for_strategy(biz_types, user_name, id_card_no, phone, user_type)
        origin_input['out_strategyBranch'] = ','.join(codes)
        # 合并新的转换变量
        origin_input.update(variables)
        strategy_request = _build_request(req_no, product_code, origin_input)
        logger.debug(strategy_request)
        # 调用决策引擎
        strategy_response = requests.post(STRATEGY_URL, json=strategy_request)
        logger.debug(strategy_response)
        if strategy_response.status_code != 200:
            raise Exception("strategyOne错误:" + strategy_response.text)
        strategy_resp = strategy_response.json()
        error = jsonpath(strategy_resp, '$..Error')
        if error:
            raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(strategy_resp, '$..Description')))
        score_to_int(strategy_resp)
        biz_types = _get_biz_types(strategy_resp)
        logger.info(biz_types)
        strategy_param['bizType'] = biz_types
        # 最后返回报告详情
        if STRATEGE_DONE in biz_types:
            detail = translate_for_report_detail(product_code, user_name, id_card_no, phone, user_type)
            json_data['reportDetail'] = [detail]
        # 处理关联人
        _relation_risk_subject(strategy_resp, out_decision_code)
        json_data['strategyResult'] = strategy_resp
        json_data['strategyInputVariables'] = variables
        json_data['rules'] = _append_rules(biz_types)
        return jsonify(json_data)
    except Exception as err:
        logger.error(str(err))
        raise ServerException(code=500, description=str(err))


def score_to_int(strategy_resp):
    resp_variables = jsonpath(strategy_resp, '$..Application.Variables')
    if resp_variables is not None:
        variables_ = resp_variables[0]
        for key, value in variables_.items():
            if key.startswith('score'):
                variables_[key] = int(round(value))


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


if __name__ == '__main__':
    logger.info('starting pipes...')
    app.run(host='0.0.0.0')
    logger.info('pipes started.')