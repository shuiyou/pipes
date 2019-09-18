import json
import traceback

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


class P001(Generate):

    def __init__(self) -> None:
        self.reponse: {}

    def shake_hand_process(self):
        try:
            json_data = request.get_json()
            logger.info("1- 》》》》》》》》》》》》》》》》》》》一级个人详版报告 defensor invoke pipes 获取bizTypes，流程开启《《《《《《《《《《《《《《《《《《《《《《《")
            logger.debug("1-1 json_data》》》》" + str(json.dumps(json_data)))
            req_no = json_data.get('reqNo')
            product_code = json_data.get('productCode')
            query_data = json_data.get('queryData')
            user_name = query_data.get('name')
            id_card_no = query_data.get('idno')
            phone = query_data.get('phone')
            user_type = query_data.get('userType')
            auth_status = query_data.get('authorStatus')
            base_type = self._get_base_type(auth_status)
            variables = T00000().run(user_name, id_card_no, phone, user_type, base_type)['variables']
            # 决策要求一直要加上00000，用户基础信息。
            variables['out_strategyBranch'] = '00000'
            logger.info("2- 》》》》》》》》》》》》》》》》》》》》》》》》》》开始策略引擎封装入参《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《")
            strategy_request = _build_request(req_no, product_code, variables=variables)
            logger.info("2-1 strategy_request》》》》" + str(strategy_request))
            logger.info("3- 》》》》》》》》》》》》》》》》》》》》》》》》》》开始调用策略引擎《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《")
            response = requests.post(STRATEGY_URL, json=strategy_request)
            if response.status_code != 200:
                raise Exception("strategyOne错误:" + response.text)
            resp_json = response.json()
            logger.info("4- 》》》》》》》》》》》》》》》》》》》》》》》》》》》策略引擎调用成功《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《")
            logger.info("4-1 resp_json》》》》" + str(resp_json))
            error = jsonpath(resp_json, '$..Error')
            if error:
                raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(resp_json, '$..Description')))
            biz_types = _get_biz_types(resp_json)
            resp = {
                'productCode': json_data.get('productCode'),
                'reqNo': json_data.get('reqNo'),
                'bizType': biz_types,
                'authorStatus':auth_status,
                'rules': _append_rules(biz_types)
            }
            self.reponse = resp
            logger.info("5- 》》》》》》》》》》》》》》》》》》》》》》》》》流程结束 pipes 回调 defensor 《《《《《《《《《《《《《《《《《《《《《《《《《《《")
            logger.info("5-1 response》》》》" + str(self.reponse))
            return self.reponse
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def strategy_process(self):
        try:
            json_data = request.get_json()
            logger.info("1- 》》》》》》》》》》》》》》》》》》》一级个人详版报告 defensor invoke pipes 获取策略引擎结果，流程开启《《《《《《《《《《《《《《《《《《《《《《《")
            logger.debug("1-1 json_data》》》》》" + str(json.dumps(json_data)))
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
            auth_status = query_data.get('authorStatus')
            biz_types = codes.copy()
            biz_types.append('00000')
            variables, out_decision_code = translate_for_strategy(biz_types, user_name, id_card_no, phone, user_type,
                                                                  auth_status)
            origin_input['out_strategyBranch'] = ','.join(codes)
            # 合并新的转换变量
            origin_input.update(variables)
            logger.info("2- 》》》》》》》》》》》》》》》》》》》》》》》》》》开始策略引擎封装入参《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《")
            strategy_request = _build_request(req_no, product_code, origin_input)
            logger.info("2-1 strategy_request》》》》" + str(strategy_request))
            logger.info("3- 》》》》》》》》》》》》》》》》》》》》》》》》》》开始调用策略引擎《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《")
            strategy_response = requests.post(STRATEGY_URL, json=strategy_request)
            if strategy_response.status_code != 200:
                raise Exception("strategyOne错误:" + strategy_response.text)
            strategy_resp = strategy_response.json()
            logger.info("4- 》》》》》》》》》》》》》》》》》》》》》》》》》》》策略引擎调用成功《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《")
            logger.info("4-1 resp_json》》》》" + str(strategy_resp))
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
                                                     'PERSONAL')
                json_data['reportDetail'] = [detail]
            # 处理关联人
            _relation_risk_subject(strategy_resp, out_decision_code)
            json_data['strategyResult'] = strategy_resp
            json_data['strategyInputVariables'] = variables
            json_data['rules'] = _append_rules(biz_types)
            self.reponse = json_data
            logger.info("5- 》》》》》》》》》》》》》》》》》》》》》》》》》流程结束 pipes 回调 defensor 《《《《《《《《《《《《《《《《《《《《《《《《《《《")
            logger.info("5-1 response》》》》" + str(self.reponse))
            return self.reponse
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def _get_base_type(self, auth_status):
        if auth_status == 'AUTHORIZED':
            return 'PERSONAL'
        else:
            return 'S_PERSONAL'
