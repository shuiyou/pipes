# @Time : 2020/10/14 9:42 AM 
# @Author : lixiaobo
# @File : p03002.py 
# @Software: PyCharm
import json
import traceback

import pandas as pd
import requests
from flask import request
from jsonpath import jsonpath

from config import STRATEGY_URL
from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.grouped_tranformer import invoke_each, invoke_union
from mapping.mapper import translate_for_strategy
from mapping.t00000 import T00000
from mapping.utils.np_encoder import NpEncoder
from product.generate import Generate
from product.p_utils import _build_request, score_to_int, _get_biz_types, _relation_risk_subject, _append_rules
from service.base_type_service_v2 import BaseTypeServiceV2
from util.type_converter import echo_var_type
from view.grouped_mapper_detail import view_variables_scheduler
from view.mapper_detail import STRATEGE_DONE

logger = LoggerUtil().logger(__name__)


class P03002(Generate):
    """
    新版一级联合报告处理逻辑
    """

    def __init__(self) -> None:
        super().__init__()
        self.response: {}

    def shake_hand_process(self):
        try:
            json_data = request.get_json()
            app_id = json_data.get('appId')
            req_no = json_data.get('reqNo')
            product_code = json_data.get('productCode')
            query_data_array = json_data.get('queryData')
            industry = json_data.get("industry")
            base_type_service = BaseTypeServiceV2(query_data_array)

            response_array = []
            for data in query_data_array:
                response_array.append(self._shake_hand_response(base_type_service, data, product_code, req_no))
            resp = {
                'appId': app_id,
                'productCode': product_code,
                'reqNo': req_no,
                'industry': industry,
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
            strategy_param = json_data.get('strategyParam')
            req_no = strategy_param.get('reqNo')
            product_code = strategy_param.get('productCode')
            query_data_array = strategy_param.get('queryData')
            subject = []
            cache_array = []
            # 遍历query_data_array调用strategy
            for data in query_data_array:
                array, resp = self._strategy_hand(data, product_code, req_no)
                subject.append(resp)
                cache_array.append(array)
                # 最后返回报告详情
            common_detail = view_variables_scheduler(product_code, json_data,
                                                     None, None, None, None, None,None, invoke_union)

            # 封装第二次调用参数
            variables = self._create_strategy_second_request(cache_array)

            # TODO
            # strategy_resp = self.invoke_strategy(variables, product_code, req_no)
            strategy_resp = self._invoke_strategy_stub()
            score_to_int(strategy_resp)
            # 封装最终返回json
            resp_end = self._create_strategy_resp(strategy_resp, variables, common_detail, subject, json_data)
            echo_var_type(None, None, resp_end)
            logger.info("response:%s", json.dumps(resp_end, cls=NpEncoder))
            self.response = resp_end
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    @staticmethod
    def _create_strategy_resp(strategy_resp, variables, common_detail, subject, json_data):
        resp = {
            'strategyInputVariables': variables,
            'strategyResult': strategy_resp,
            'commonDetail': common_detail,
            'subject': subject
        }
        return resp

    @staticmethod
    def _create_strategy_second_request(cache_array):
        """
        :param cache_array:
        :return:
        """
        df = pd.DataFrame(cache_array)
        # 取前10行数据
        df_person = df.query('userType=="PERSONAL" and strategy=="01"') \
            .sort_values(by=["fundratio"], ascending=False)

        # 删除重复的数据
        df_person.drop_duplicates(subset=["name", "idno"], inplace=True)

        logger.info("-------df_person\n%s", df_person)

        # 拼接入参variables
        variables = {}
        person_index = 0
        for index, row in df_person.iterrows():
            person_index = person_index + 1
            variables['score_p' + str(person_index)] = row['score']
            variables['score_fraud_p' + str(person_index)] = row['score_fraud']
            variables['score_owner_p' + str(person_index)] = row['score_owner']
            variables['score_bus_p' + str(person_index)] = row['score_bus']
            variables['score_black_p' + str(person_index)] = row['score_black']
            variables['score_fin_p' + str(person_index)] = row['score_fin']
            variables['model_pred_p' + str(person_index)] = row['model_pred']
        variables['base_type'] = 'UNION'
        return variables

    def _strategy_hand(self, data, product_code, req_no):
        user_name = data.get('name')
        id_card_no = data.get('idno')
        phone = data.get('phone')
        user_type = data.get('userType')
        codes = data.get('bizType')
        base_type = data.get('baseType')
        relation = data.get('relation')
        fund_ratio = data.get('fundratio')
        biz_types = codes.copy()
        biz_types.append('00000')
        variables, out_decision_code = translate_for_strategy(product_code, biz_types, user_name, id_card_no, phone,
                                                              user_type, base_type, self.df_client, data)
        origin_input = data.get('strategyInputVariables')
        if origin_input is None:
            origin_input = {}
        origin_input['out_strategyBranch'] = ','.join(codes)
        # 合并新的转换变量
        origin_input.update(variables)

        # TODO
        strategy_resp = self.invoke_strategy(origin_input, product_code, req_no)
        # strategy_resp = self._invoke_strategy_stub()
        score_to_int(strategy_resp)
        biz_types, categories = _get_biz_types(strategy_resp)

        resp = {}
        self._calc_view_variables(base_type, biz_types, data, id_card_no, out_decision_code, phone, product_code,
                                  resp, strategy_resp, user_name, user_type, variables)
        array = self._get_strategy_second_array(data, fund_ratio, relation, strategy_resp, user_name, user_type,
                                                variables)
        return array, resp

    @staticmethod
    def invoke_strategy(variables, product_code, req_no):
        strategy_request = _build_request(req_no, product_code, variables)
        logger.info("strategy_request:%s", strategy_request)
        strategy_response = requests.post(STRATEGY_URL, json=strategy_request)
        logger.debug("strategy_response%s", strategy_response)
        if strategy_response.status_code != 200:
            raise Exception("strategyOne错误:" + strategy_response.text)
        strategy_resp = strategy_response.json()
        error = jsonpath(strategy_resp, '$..Error')
        if error:
            raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(strategy_resp, '$..Description')))
        return strategy_resp

    def _get_strategy_second_array(self, data, fundratio, relation, strategy_resp, user_name, user_type,
                                   variables):
        array = {
            'name': user_name,
            'idno': data.get('idno'),
            'userType': user_type,
            'strategy': data.get("extraParam").get("strategy")
        }
        if fundratio is None or fundratio == '':
            array['fundratio'] = 0.00
        else:
            array['fundratio'] = float(fundratio)

        array['relation'] = relation
        array['per_face_relent_indusCode1'] = self._get_json_path_value(variables, '$..per_face_relent_indusCode1')
        array['com_bus_face_outwardindusCode1'] = self._get_json_path_value(variables,
                                                                            '$..com_bus_face_outwardindusCode1')
        array['com_bus_industrycode'] = self._get_json_path_value(variables, '$..com_bus_industrycode')
        array['score_black'] = self._get_json_path_value(strategy_resp, '$..score_black')
        array['score_owner'] = self._get_json_path_value(strategy_resp, '$..score_owner')
        array['score_fin'] = self._get_json_path_value(strategy_resp, '$..score_fin')
        array['score_fraud'] = self._get_json_path_value(strategy_resp, '$..score_fraud')
        array['score_bus'] = self._get_json_path_value(strategy_resp, '$..score_bus')
        array['score'] = self._get_json_path_value(strategy_resp, '$..score')
        array['model_pred'] = self._get_json_path_value(strategy_resp, '$..model_pred')
        array["id"] = data.get("id")
        array["parentId"] = data.get("parentId")
        return array

    @staticmethod
    def _get_json_path_value(strategy_resp, path):
        res = jsonpath(strategy_resp, path)
        if isinstance(res, list) and len(res) > 0:
            return res[0]
        else:
            return 0

    @staticmethod
    def _calc_view_variables(base_type, biz_types, data, id_card_no, out_decision_code, phone, product_code,
                             resp, strategy_resp, user_name, user_type, variables):
        """
        每次循环后封装每个主体的resp信息
        """
        data['strategyInputVariables'] = variables
        if STRATEGE_DONE in biz_types:
            detail = view_variables_scheduler(product_code, None, user_name, id_card_no, phone, user_type, base_type,
                                              data, invoke_each)
            resp['reportDetail'] = [detail]
        # 处理关联人
        _relation_risk_subject(strategy_resp, out_decision_code)
        resp['strategyResult'] = strategy_resp
        resp['rules'] = _append_rules(biz_types)
        resp['queryData'] = data

    def _shake_hand_response(self, base_type_service, data, product_code, req_no):
        """
        和决策交互，封装response
        :param data:
        :param product_code:
        :param req_no:
        :return:
        """
        user_name = data.get('name')
        id_card_no = data.get('idno')
        phone = data.get('phone')
        user_type = data.get('userType')
        # 获取base_type
        base_type = base_type_service.parse_base_type(data)
        variables = T00000().run(user_name, id_card_no, phone, user_type, base_type, data)['variables']
        # 决策要求一直要加上00000，用户基础信息。
        variables["product_code"] = product_code
        variables['out_strategyBranch'] = '00000'
        logger.info("variables:%s", variables)

        # TODO
        resp_json = self.invoke_strategy(variables, product_code, req_no)
        # resp_json = self._invoke_strategy_stub()
        biz_types, categories = _get_biz_types(resp_json)
        rules = _append_rules(biz_types)

        resp = {}
        resp.update(data)
        resp['baseType'] = base_type
        resp['bizType'] = biz_types
        resp['rules'] = rules
        resp['categories'] = categories

        return resp

    @staticmethod
    def _invoke_shake_hand_stub():
        resp = '''
            {"StrategyOneResponse": {"Header": {"InquiryCode": "Q394106614698901504", "ProcessCode": "Level1_m", "OrganizationCode": "", "ProcessVersion": 94, "LayoutVersion": 20}, "Body": {"Application": {"Variables": {"out_strategyBranch": "00001,01001,02001,05001,05002,06001,07001,10001,11001,12001,13001,14001,16001,17001,18001,31001", "out_isQuery": "Y", "out_result": "A"}, "Categories": [{"Reason": {"Variables": {"out_decisionBranchCode": "AT001", "out_ReasonCode": "RY203"}}}]}}}}
            '''
        return json.loads(resp)

    @staticmethod
    def _invoke_strategy_stub():
        resp = '''
            {"StrategyOneResponse": {"Header": {"InquiryCode": "Q412316320361775104", "ProcessCode": "Level1_m", "OrganizationCode": "", "ProcessVersion": 94, "LayoutVersion": 20}, "Body": {"Application": {"Variables": {"out_strategyBranch": "fffff", "out_isQuery": "N", "score_fraud": 100, "score_debit": 31.84, "score_credit": 85, "score_black": 100, "score": 100, "SCORE_GE_RAW": 74, "out_result": "A", "level": "高", "level_black": "高", "level_credit": "高", "level_debit": "中", "level_fraud": "高", "l_m_critical_score": 30, "m_h_critical_score": 70}, "Categories": [{"Reason": {"Variables": {"out_decisionBranchCode": "J002", "out_ReasonCode": "RR205"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "J004", "out_ReasonCode": "RR205"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "J007", "out_ReasonCode": "RR206"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "J008", "out_ReasonCode": "RR206"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "JT002", "out_ReasonCode": "RY205"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "JT003", "out_ReasonCode": "RY205"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "JT006", "out_ReasonCode": "RY205"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "JT008", "out_ReasonCode": "RY205"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "JT010", "out_ReasonCode": "RY205"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "S001", "out_ReasonCode": "RR205"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "F001", "out_ReasonCode": "RR401"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "HT002", "out_ReasonCode": "RY101"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "HT007", "out_ReasonCode": "RY202"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "HT027", "out_ReasonCode": "RY501"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "HT032", "out_ReasonCode": "RY501"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "HT037", "out_ReasonCode": "RY205"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "MT004", "out_ReasonCode": "RY103"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "NT001", "out_ReasonCode": "RY501"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "WT001", "out_ReasonCode": "RY301"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "WT002", "out_ReasonCode": "RY301"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "JT012", "out_ReasonCode": "RR201"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "ZM001", "out_ReasonCode": "RR209"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "ZM002", "out_ReasonCode": "RR403"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "ZM004", "out_ReasonCode": "RR503"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "ZM006", "out_ReasonCode": "RR601"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "ZT003", "out_ReasonCode": "RY104"}}}]}}}}
            '''
        return json.loads(resp)
