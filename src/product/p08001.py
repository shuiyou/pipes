# @Time : 2020/6/18 8:00 PM 
# @Author : lixiaobo
# @File : p08001.py 
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
from portrait.portrait_processor import PortraitProcessor
from portrait.transflow.single_portrait import SinglePortrait
from portrait.transflow.union_portrait import UnionPortrait
from product.generate import Generate
from product.p_config import product_codes_dict
from product.p_utils import _build_request, score_to_int, _get_biz_types, _relation_risk_subject, _append_rules
from service.base_type_service_v3 import BaseTypeServiceV3
from view.mapper_detail import translate_for_report_detail

logger = LoggerUtil().logger(__name__)


# 流水报告产品处理
class P08001(Generate):
    def __init__(self) -> None:
        super().__init__()
        self.response: {}

    def shake_hand_process(self):
        """
        json_data主体的关联关系
        需要根据关联关系，处理**portrait的相关数据
        """
        try:
            json_data = request.get_json()
            logger.info("json_data:%s", json.dumps(json_data))

            req_no = json_data.get('reqNo')
            report_req_no = json_data.get("reportReqNo")
            product_code = json_data.get('productCode')
            is_single = json_data.get("single")
            query_data_array = json_data.get('queryData')

            base_type_service = BaseTypeServiceV3(query_data_array)

            main_node = None
            response_array = []
            for data in query_data_array:
                base_type = base_type_service.parse_base_type(data)
                data["baseType"] = base_type
                if data.get("relation") == "MAIN":
                    main_node = data
                else:
                    response_array.append(data)

            resp = self._query_entity_hand_shake(json_data, main_node, req_no, report_req_no, is_single,
                                                 query_data_array)
            response_array.append(resp)

            resp = {
                'reqNo': req_no,
                'reportReqNo': report_req_no,
                'productCode': product_code,
                "single": is_single,
                'queryData': response_array
            }
            self.response = resp
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def _query_entity_hand_shake(self, json_data, data, req_no, report_req_no, is_single, query_data_array):
        """
        流水握手的相关信息处理
        """
        cached_data = {}
        user_name = data.get('name')
        id_card_no = data.get('idno')
        phone = data.get('phone')
        bank_name = data.get('bankName')
        bank_account = data.get('bankAccount')
        user_type = data.get('userType')
        base_type = data.get("baseType")

        var_item = {
            "bizType": product_codes_dict[json_data.get("productCode")]
        }

        var_item.update(data)
        portrait_processor = self._obtain_portrait_processor(is_single)
        portrait_processor.init(var_item, query_data_array, user_name, user_type, base_type, id_card_no, phone, bank_name, bank_account, data, cached_data)
        portrait_processor.process()

        return var_item

    def strategy_process(self):
        # 获取请求参数
        try:
            json_data = request.get_json()
            logger.info("1. 流水报告：获取策略引擎结果，流程开启, 入参为：%s", json.dumps(json_data))
            strategy_param = json_data.get('strategyParam')
            req_no = strategy_param.get('reqNo')
            product_code = strategy_param.get('productCode')
            step_req_no = strategy_param.get('stepReqNo')
            version_no = strategy_param.get('versionNo')
            pre_report_req_no = strategy_param.get('preReportReqNo')
            query_data_array = strategy_param.get('queryData')
            is_single = strategy_param.get("single")

            # 遍历query_data_array调用strategy
            base_type_service = BaseTypeServiceV3(query_data_array)
            main_query_data = None
            subjects = []
            for data in query_data_array:
                data["preReportReqNo"] = pre_report_req_no
                data["baseTypeDetail"] = base_type_service.parse_base_type(data)
                subjects.append(data)

                if data.get("relation") == "MAIN":
                    main_query_data = data

            # 决策调用及view变量清洗
            resp = self.strategy(is_single, self.df_client, subjects, main_query_data, product_code, req_no)

            item_data_list = []
            for subject in subjects:
                item_data = {
                    "queryData": subject
                }

                if subject.get("relation") == "MAIN":
                    item_data.update(resp)

                item_data_list.append(item_data)

            self.response = self.create_strategy_resp(product_code, req_no, step_req_no, version_no, item_data_list)

            logger.info("2. 流水报告，应答：%s", json.dumps(self.response))
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def strategy(self, is_single, df_client, subjects, main_query_data, product_code, req_no):
        user_name = main_query_data.get('name')
        id_card_no = main_query_data.get('idno')
        phone = main_query_data.get('phone')
        user_type = main_query_data.get('userType')
        codes = product_codes_dict[product_code]
        base_type = self.calc_base_type(user_type)
        biz_types = codes.copy()
        biz_types.append('00000')
        data_repository = {"input_param": subjects}
        variables, out_decision_code = translate_for_strategy(product_code, biz_types, user_name, id_card_no, phone,
                                                              user_type, base_type, df_client, main_query_data, data_repository)
        origin_input = {'out_strategyBranch': ','.join(codes)}
        # 合并新的转换变量
        origin_input.update(variables)
        variables["single"] = is_single
        logger.info("1. 流水报告-开始策略引擎封装入参")
        strategy_request = _build_request(req_no, product_code, origin_input)
        logger.info("2. 流水报告-策略引擎封装入参:%s", strategy_request)
        strategy_response = requests.post(STRATEGY_URL, json=strategy_request)
        logger.info("3. 流水报告-策略引擎返回结果：%s", strategy_response)

        status_code = strategy_response.status_code
        if status_code != 200:
            raise Exception("strategyOne错误:" + strategy_response.text)
        strategy_resp = strategy_response.json()
        logger.info("4. 流水报告-策略引擎调用成功 %s", strategy_resp)
        error = jsonpath(strategy_resp, '$..Error')
        if error:
            raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(strategy_resp, '$..Description')))
        score_to_int(strategy_resp)
        biz_types, categories = _get_biz_types(strategy_resp)
        logger.info(biz_types)

        resp = {}

        main_query_data['bizType'] = biz_types
        main_query_data["baseType"] = base_type
        main_query_data['strategyInputVariables'] = variables
        # 最后返回报告详情
        detail = translate_for_report_detail(product_code, user_name, id_card_no, phone, user_type,
                                             base_type, main_query_data, data_repository)
        resp['reportDetail'] = [detail]
        # 处理关联人
        _relation_risk_subject(strategy_resp, out_decision_code)
        resp['strategyResult'] = strategy_resp
        resp['rules'] = _append_rules(biz_types)

        data_repository.clear()
        del data_repository
        return resp

    @staticmethod
    def calc_base_type(user_type):
        if user_type == "PERSONAL":
            return "U_PERSONAL"
        elif user_type == "COMPANY":
            return "U_COMPANY"
        else:
            raise ServerException(code=400, description="不识别的用户类型:" + user_type)

    @staticmethod
    def _obtain_portrait_processor(is_single) -> PortraitProcessor:
        if is_single:
            return SinglePortrait()
        else:
            return UnionPortrait()

