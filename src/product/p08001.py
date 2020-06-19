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
from service.base_type_service import BaseTypeService
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

            report_req_no = json_data.get("reportReqNo")
            product_code = json_data.get('productCode')
            is_single = json_data.get("isSingle")
            query_data_array = json_data.get('queryData')
            req_no = json_data.get('reqNo')
            base_type_service = BaseTypeService(query_data_array)

            response_array = []
            cached_data = {}
            # 遍历query_data_array调用strategy
            for data in query_data_array:
                user_name = data.get('name')
                id_card_no = data.get('idno')
                phone = data.get('phone')
                user_type = data.get('userType')

                var_item = {}
                portrait_processor = self._obtain_portrait_processor(is_single)
                portrait_processor.init(var_item, user_name, id_card_no, data, cached_data)
                portrait_processor.process()

                response_array.append(var_item)

            resp = {
                'productCode': product_code,
                'reqNo': req_no,
                'queryData': response_array
            }
            self.response = resp
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

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
            subject = []

            # 遍历query_data_array调用strategy
            index = 0
            total = len(query_data_array)
            for data in query_data_array:
                index = index + 1
                logger.info("P08001_process------------" + str(index) + "/" + str(total))
                data["preReportReqNo"] = pre_report_req_no;
                resp = self.strategy(self.df_client, data, product_code, req_no)
                subject.append(resp)

            self.response = self.create_strategy_resp(product_code, req_no, step_req_no, version_no, subject)

            logger.info("2. 流水报告，应答：%s", json.dumps(self.response))
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def strategy(self, df_client, data, product_code, req_no):
        user_name = data.get('name')
        id_card_no = data.get('idno')
        phone = data.get('phone')
        user_type = data.get('userType')
        codes = product_codes_dict[product_code]
        base_type = self.calc_base_type(user_type)
        biz_types = codes.copy()
        data_repository = {}
        variables, out_decision_code = translate_for_strategy(product_code, biz_types, user_name, id_card_no, phone,
                                                              user_type, base_type, df_client, data, data_repository)
        origin_input = {'out_strategyBranch': ','.join(codes)}
        # 合并新的转换变量
        origin_input.update(variables)
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
        self._strategy_second_loop_resp(base_type, biz_types, data, id_card_no, out_decision_code, phone, product_code,
                                        resp, strategy_resp, user_name, user_type, variables, data_repository)
        data_repository.clear()
        del data_repository
        return resp

    @staticmethod
    def _strategy_second_loop_resp(base_type, biz_types, data, id_card_no, out_decision_code, phone, product_code,
                                   resp, strategy_resp, user_name, user_type, variables, data_repository):
        data['bizType'] = biz_types
        data['strategyInputVariables'] = variables
        # 最后返回报告详情
        # if STRATEGE_DONE in biz_types:
        detail = translate_for_report_detail(product_code, user_name, id_card_no, phone, user_type,
                                             base_type, data, data_repository)
        resp['reportDetail'] = [detail]
        # 处理关联人
        _relation_risk_subject(strategy_resp, out_decision_code)
        resp['strategyResult'] = strategy_resp
        resp['rules'] = _append_rules(biz_types)
        resp['queryData'] = data

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
