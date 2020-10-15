# @Time : 2020/10/14 9:42 AM 
# @Author : lixiaobo
# @File : p03002.py 
# @Software: PyCharm
import traceback

import pandas as pd
import requests
from flask import request
from jsonpath import jsonpath

from config import STRATEGY_URL
from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.mapper import translate_for_strategy
from mapping.t00000 import T00000
from product.generate import Generate
from product.p_utils import _build_request, score_to_int, _get_biz_types, _relation_risk_subject, _append_rules
from service.base_type_service_v2 import BaseTypeServiceV2
from view.mapper_detail import STRATEGE_DONE, translate_for_report_detail

logger = LoggerUtil().logger(__name__)


class P03002(Generate):
    """s
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
            step_req_no = strategy_param.get('stepReqNo')
            version_no = strategy_param.get('versionNo')
            query_data_array = strategy_param.get('queryData')
            subject = []
            cache_array = []
            # 遍历query_data_array调用strategy
            for data in query_data_array:
                array, resp = self._strategy_hand(data, product_code, req_no)
                subject.append(resp)
                cache_array.append(array)
            # 封装第二次调用参数
            variables = self._create_strategy_second_request(cache_array)
            strategy_request = _build_request(req_no, product_code, variables=variables)
            strategy_response = requests.post(STRATEGY_URL, json=strategy_request)
            if strategy_response.status_code != 200:
                raise Exception("strategyOne错误:" + strategy_response.text)
            strategy_resp = strategy_response.json()
            error = jsonpath(strategy_resp, '$..Error')
            if error:
                raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(strategy_resp, '$..Description')))
            score_to_int(strategy_resp)
            # 封装最终返回json
            resp_end = self._create_strategy_resp(product_code, req_no, step_req_no, strategy_resp, variables,
                                                  version_no, subject)
            self.response = resp_end
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def _create_strategy_resp(self, product_code, req_no, step_req_no, strategy_resp, variables, version_no, subject):
        resp = {}
        resp['reqNo'] = req_no
        resp['product_code'] = product_code
        resp['stepReqNo'] = step_req_no
        resp['versionNo'] = version_no
        resp['strategyInputVariables'] = variables
        resp['strategyResult'] = strategy_resp
        resp['subject'] = subject
        return resp

    def _create_strategy_second_request(self, cache_array):
        """
        挑选最多10个个人主体和10个企业主体封装入参
        :param cache_array:
        :return:
        """
        df = pd.DataFrame(cache_array)
        if df.query('relation == "MAIN" and userType == "PERSONAL"').shape[0] > 0:
            # 借款主体为个人-联合报告-排序
            self._sort_union_person_df(df)
        elif df.query('relation == "MAIN" and userType == "COMPANY"').shape[0] > 0:
            # 借款主体为企业-联合报告-排序
            self._sort_union_company_df(df)
        else:
            raise ServerException(code=500, description=str('没有借款主体'))

        # 取前10行数据
        df_person = df.query('userType=="PERSONAL"').sort_values(by=["fundratio"],
                                                                                  ascending=False).sort_values(
            by=["order"], ascending=True)
        df_company = df.query('userType=="COMPANY"').sort_values(by=["fundratio"],
                                                                                 ascending=False).sort_values(
            by=["order"], ascending=True)

        # 删除重复的数据
        df_person.drop_duplicates(subset=["name", "idno"], inplace=True)
        df_company.drop_duplicates(subset=["name", "idno"], inplace=True)

        logger.info("-------df_person\n%s", df_person)
        logger.info("-------df_company\n%s", df_company)

        # 拼接入参variables
        logger.info("----拼接入参variables---df_person\n%s", df_person[0: 10])
        logger.info("----拼接入参variables---df_company\n%s", df_company[0: 10])
        variables = self._strategy_second_request_variables(df_company[0: 10], df_person[0: 10])
        return variables

    def _strategy_second_request_variables(self, df_compay, df_person):
        variables = {}
        person_index = 0
        company_index = 0
        phycode_array = []
        for index, row in df_person.iterrows():
            person_index = person_index + 1
            variables['score_black_a' + str(person_index)] = row['score_black']
            variables['score_credit_a' + str(person_index)] = row['score_credit']
            variables['score_debit_a' + str(person_index)] = row['score_debit']
            variables['score_fraud_a' + str(person_index)] = row['score_fraud']
            variables['score_a' + str(person_index)] = row['score']
            self._phycode_type_array(
                [row['per_face_relent_indusCode1'], row['com_bus_face_outwardindusCode1'], row['com_bus_industrycode']],
                phycode_array)
        for index, row in df_compay.iterrows():
            company_index = company_index + 1
            variables['score_black_c' + str(company_index)] = row['score_black']
            variables['score_business_c' + str(company_index)] = row['score_business']
            variables['score_c' + str(company_index)] = row['score']
            self._phycode_type_array(
                [row['per_face_relent_indusCode1'], row['com_bus_face_outwardindusCode1'], row['com_bus_industrycode']],
                phycode_array)
        variables['u_industryphycode'] = len(phycode_array)
        variables['base_type'] = 'UNION'
        return variables

    def _phycode_type_array(self, values, array):
        for value in values:
            if value is not None and value != '' and value not in array:
                array.append(value)

    def _strategy_hand(self, data, product_code, req_no):
        resp = {}
        array = {}
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
        strategy_request = _build_request(req_no, product_code, origin_input)
        strategy_response = requests.post(STRATEGY_URL, json=strategy_request)
        logger.debug(strategy_response)
        if strategy_response.status_code != 200:
            raise Exception("strategyOne错误:" + strategy_response.text)
        strategy_resp = strategy_response.json()
        error = jsonpath(strategy_resp, '$..Error')
        if error:
            raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(strategy_resp, '$..Description')))
        score_to_int(strategy_resp)
        biz_types, categories = _get_biz_types(strategy_resp)
        logger.info(biz_types)
        self._strategy_second_loop_resp(base_type, biz_types, data, id_card_no, out_decision_code, phone, product_code,
                                        resp, strategy_resp, user_name, user_type, variables)
        self._get_strategy_second_array(array, data, fundratio, relation, strategy_resp, user_name, user_type, variables)
        return array, resp

    def _get_strategy_second_array(self, array, data, fundratio, relation, strategy_resp, user_name, user_type, variables):
        array['name'] = user_name
        array['idno'] = data.get('idno')
        array['userType'] = user_type
        if fundratio is not None and fundratio != '':
            array['fundratio'] = float(fundratio)
        else:
            array['fundratio'] = 0.00
        array['relation'] = relation
        array['per_face_relent_indusCode1'] = self._get_json_path_value(variables, '$..per_face_relent_indusCode1')
        array['com_bus_face_outwardindusCode1'] = self._get_json_path_value(variables,
                                                                            '$..com_bus_face_outwardindusCode1')
        array['com_bus_industrycode'] = self._get_json_path_value(variables, '$..com_bus_industrycode')
        array['score_black'] = self._get_json_path_value(strategy_resp, '$..score_black')
        array['score_credit'] = self._get_json_path_value(strategy_resp, '$..score_credit')
        array['score_debit'] = self._get_json_path_value(strategy_resp, '$..score_debit')
        array['score_fraud'] = self._get_json_path_value(strategy_resp, '$..score_fraud')
        array['score_business'] = self._get_json_path_value(strategy_resp, '$..score_business')
        array['score'] = self._get_json_path_value(strategy_resp, '$..score')
        array["id"] = data.get("id")
        array["parentId"] = data.get("parentId")

    def _get_json_path_value(self, strategy_resp, path):
        res = jsonpath(strategy_resp, path)
        if isinstance(res, list) and len(res) > 0:
            return res[0]
        else:
            return 0

    def _strategy_second_loop_resp(self, base_type, biz_types, data, id_card_no, out_decision_code, phone, product_code,
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
        if STRATEGE_DONE in biz_types and user_type == "PERSONAL":
            detail = translate_for_report_detail(product_code, user_name, id_card_no, phone, user_type,
                                                 base_type)
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
        strategy_request = _build_request(req_no, product_code, variables=variables)
        # 调用决策引擎
        response = requests.post(STRATEGY_URL, json=strategy_request)
        if response.status_code != 200:
            raise Exception("strategyOne错误:" + response.text)
        resp_json = response.json()
        error = jsonpath(resp_json, '$..Error')
        if error:
            raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(resp_json, '$..Description')))
        biz_types, categories = _get_biz_types(resp_json)
        rules = _append_rules(biz_types)

        resp = {}
        resp.update(data)
        resp['baseType'] = base_type
        resp['bizType'] = biz_types
        resp['rules'] = rules
        resp['categories'] = categories

        return resp

