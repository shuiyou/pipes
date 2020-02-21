# -*- coding: utf-8 -*-
# @Time : 2020/2/20 1:53 PM
# @Author : lixiaobo
# @Site : 
# @File : p06001.py
# @Software: PyCharm
import json
import traceback

import requests
from flask import request
from jsonpath import jsonpath

from config import STRATEGY_URL
from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.mapper import translate_for_strategy
from product.common.hand_shake_service import HandShakeService
from product.generate import Generate
from product.p_utils import _build_request, _get_biz_types, _append_rules, score_to_int, _relation_risk_subject
from service.base_type_service import BaseTypeService
from view.mapper_detail import STRATEGE_DONE, translate_for_report_detail

logger = LoggerUtil().logger(__name__)


# 贷后处理
class P06001(Generate):
    def __init__(self) -> None:
        super().__init__()
        self.response: {}

    def shake_hand_process(self):
        try:
            json_data = request.get_json()
            logger.info("1. 贷后报告决策握手处理开始，入参为：%s", json.dumps(json_data))
            req_no = json_data.get('reqNo')
            product_code = json_data.get('productCode')
            query_data_array = json_data.get('queryData')

            base_type_service = BaseTypeService(query_data_array)
            hand_shake_service = HandShakeService()

            response_array = []
            # 遍历query_data_array调用strategy
            for data in query_data_array:
                resp = hand_shake_service.hand_shake(base_type_service, data, product_code, req_no)
                response_array.append(resp)
            final_resp = {
                'productCode': product_code,
                'reqNo': req_no,
                'queryData': response_array
            }
            self.response = final_resp
            logger.info("2. 贷后报告决策握手流程结束 应答给defensor报文为：%s", json.dumps(self.response))
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def strategy_process(self):
        # 获取请求参数
        try:
            json_data = request.get_json()
            logger.info("1. 贷后策略：获取策略引擎结果，流程开启, 入参为：%s", json.dumps(json_data))
            strategy_param = json_data.get('strategyParam')
            req_no = strategy_param.get('reqNo')
            product_code = strategy_param.get('productCode')
            step_req_no = strategy_param.get('stepReqNo')
            version_no = strategy_param.get('versionNo')
            query_data_array = strategy_param.get('queryData')
            subject = []
            cache_array = []
            # 遍历query_data_array调用strategy
            for data in query_data_array:
                resp = self._strategy_second_hand(data, product_code, req_no)
                subject.append(resp)

            self.response = self._create_strategy_resp(product_code, req_no, step_req_no, version_no, subject)

            logger.info("2. 贷后策略：贷后策略调用，应答：%s", json.dumps(self.response))
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def _strategy_second_hand(self, data, product_code, req_no):
        origin_input = data.get('strategyInputVariables')
        if origin_input is None:
            origin_input = {}
        user_name = data.get('name')
        id_card_no = data.get('idno')
        phone = data.get('phone')
        user_type = data.get('userType')
        codes = data.get('bizType')
        base_type = data.get('baseType')
        relation = data.get('relation')
        fundratio = data.get('fundratio')
        biz_types = codes.copy()
        biz_types.append('00000')
        variables, out_decision_code = translate_for_strategy(product_code, biz_types, user_name, id_card_no, phone,
                                                              user_type, base_type, self.df_client, data)
        origin_input['out_strategyBranch'] = ','.join(codes)
        # 合并新的转换变量
        origin_input.update(variables)
        logger.info("1. 开始策略引擎封装入参")
        strategy_request = _build_request(req_no, product_code, origin_input)
        logger.info("2. 策略引擎封装入参:%s", strategy_request)
        strategy_response = requests.post(STRATEGY_URL, json=strategy_request)
        logger.info("3. 策略引擎返回结果：%s", strategy_response)
        if strategy_response.status_code != 200:
            raise Exception("strategyOne错误:" + strategy_response.text)
        strategy_resp = strategy_response.json()
        logger.info("4. 策略引擎调用成功 %s", strategy_resp)
        error = jsonpath(strategy_resp, '$..Error')
        if error:
            raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(strategy_resp, '$..Description')))
        score_to_int(strategy_resp)
        biz_types, categories = _get_biz_types(strategy_resp)
        logger.info(biz_types)

        resp = {}
        self._strategy_second_loop_resp(base_type, biz_types, data, id_card_no, out_decision_code, phone, product_code,
                                        resp, strategy_resp, user_name, user_type, variables)
        return resp

    @staticmethod
    def _strategy_second_loop_resp(base_type, biz_types, data, id_card_no, out_decision_code, phone, product_code,
                                   resp, strategy_resp, user_name, user_type, variables):
        """
        每次循环后封装每个主体的resp信息
        :param base_type:
        :param biz_types:
        :param data:
        :param id_card_no:
        :param out_decision_code:
        :param phone:
        :param product_code:
        :param resp:
        :param strategy_resp:
        :param user_name:
        :param user_type:
        :param variables:
        :return:
        """
        data['bizType'] = biz_types
        data['strategyInputVariables'] = variables
        # 最后返回报告详情
        if STRATEGE_DONE in biz_types:
            detail = translate_for_report_detail(product_code, user_name, id_card_no, phone, user_type,
                                                 base_type)
            resp['reportDetail'] = [detail]
        # 处理关联人
        _relation_risk_subject(strategy_resp, out_decision_code)
        resp['strategyResult'] = strategy_resp
        resp['rules'] = _append_rules(biz_types)
        resp['queryData'] = data

    @staticmethod
    def _create_strategy_resp(product_code, req_no, step_req_no, version_no, subject):
        resp = {}
        resp['reqNo'] = req_no
        resp['product_code'] = product_code
        resp['stepReqNo'] = step_req_no
        resp['versionNo'] = version_no
        resp['subject'] = subject
        return resp

    @staticmethod
    def _get_json_path_value(strategy_resp, path):
        res = jsonpath(strategy_resp, path)
        if isinstance(res, list) and len(res) > 0:
            return res[0]
        else:
            return 0

    @staticmethod
    def calc_base_type(base_type_service, subject):
        return base_type_service.parse_base_type(subject)
