# -*- coding: utf-8 -*-
# @Time : 2020/2/21 9:35 AM
# @Author : lixiaobo
# @Site : 
# @File : shake_hand_service.py.py
# @Software: PyCharm
import requests
from jsonpath import jsonpath

from config import STRATEGY_URL
from logger.logger_util import LoggerUtil
from mapping.t00000 import T00000
from product.p_utils import _build_request, _append_rules, _get_biz_types

logger = LoggerUtil().logger(__name__)


# 握手服务管理
class HandShakeService(object):
    def __init__(self):
        pass

    def hand_shake(self, base_type_service, data, product_code, req_no):
        """
        和决策交互，封装response
        :param base_type_service:
        :param data:
        :param product_code:
        :param req_no:
        :return:
        """
        resp = {}
        user_name = data.get('name')
        id_card_no = data.get('idno')
        phone = data.get('phone')
        user_type = data.get('userType')
        auth_status = data.get('authorStatus')
        fund_ratio = data.get('fundratio')
        apply_amount = data.get("applyAmo")
        relation = data.get('relation')
        self_id = data.get('id')
        parent_id = data.get("parentId")
        # 获取base_type
        base_type = self.calc_base_type(base_type_service, data)
        variables = T00000().run(user_name, id_card_no, phone, user_type, base_type)['variables']
        # 决策要求一直要加上00000，用户基础信息。
        variables["product_code"] = product_code
        variables['out_strategyBranch'] = '00000'
        logger.info("1. 开始策略引擎封装入参")
        strategy_request = _build_request(req_no, product_code, variables=variables)
        logger.info("2. 决策调用入参为：%s", strategy_request)
        # 调用决策引擎
        logger.info("3. 开始调用策略引擎")
        response = requests.post(STRATEGY_URL, json=strategy_request)
        if response.status_code != 200:
            raise Exception("strategyOne错误:" + response.text)
        resp_json = response.json()
        logger.info("4. 策略引擎调用成功, 应答为：%s", resp_json)
        error = jsonpath(resp_json, '$..Error')
        if error:
            raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(resp_json, '$..Description')))
        biz_types, categories = _get_biz_types(resp_json)
        rules = _append_rules(biz_types)
        resp['name'] = user_name
        resp['idno'] = id_card_no
        resp['phone'] = phone
        resp['userType'] = user_type
        resp['authStatus'] = auth_status
        resp['fundratio'] = fund_ratio
        resp["applyAmo"] = apply_amount
        resp['baseType'] = base_type
        resp['relation'] = relation
        resp['bizType'] = biz_types
        resp['rules'] = rules
        resp['categories'] = categories
        resp['id'] = self_id
        resp['parentId'] = parent_id

        return resp

    @staticmethod
    def calc_base_type(base_type_service, subject):
        """
        :param subject:
        :type base_type_service: object
        """
        return base_type_service.parse_base_type(subject)
