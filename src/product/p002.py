
import json
import requests
from flask import request, jsonify
from jsonpath import jsonpath


from config import STRATEGY_URL
from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.mapper import translate_for_strategy

from mapping.t00000 import T00000
from product.generate import Generate
from product.p_utils import _build_request, _get_biz_types, _append_rules, score_to_int, _relation_risk_subject
from view.mapper_detail import STRATEGE_DONE, translate_for_report_detail

logger = LoggerUtil().logger(__name__)



class P002(Generate):


    def shack_hander_process(self):
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


    def strategy_process(self):
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
            variables, out_decision_code = translate_for_strategy(biz_types, user_name, id_card_no, phone, user_type,
                                                                  'COMPANY')
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
                detail = translate_for_report_detail(product_code, user_name, id_card_no, phone, user_type,
                                                     'COMPANY')
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
