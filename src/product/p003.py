import json
import traceback
from pydoc import describe

import requests
from flask import request, jsonify
from jsonpath import jsonpath

from config import STRATEGY_URL
from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.mapper import translate_for_strategy

from mapping.t00000 import T00000
from product.generate import Generate
from product.p_utils import _build_request, _get_biz_types, _append_rules, score_to_int, _relation_risk_subject, \
    _get_thread_id
from util.common_util import exception
from view.mapper_detail import STRATEGE_DONE, translate_for_report_detail
import pandas as pd

logger = LoggerUtil().logger(__name__)


class P003(Generate):

    def __init__(self) -> None:
        self.reponse: {}

    def shake_hand_process(self):
        try:
            json_data = request.get_json()
            logger.info("1- 》》》》》》》》》》》》》》》》》》》一级联合报告 defensor invoke pipes 获取bizTypes，流程开启《《《《《《《《《《《《《《《《《《《《《《《")
            logger.debug("1-1 json_data》》》》"+str(json.dumps(json_data)))
            req_no = json_data.get('reqNo')
            product_code = json_data.get('productCode')
            query_data_array = json_data.get('queryData')
            response_array = []
            # 遍历query_data_array调用strategy
            for data in query_data_array:
                response_array.append(self._shack_hander_response(data, product_code, req_no))
            resp = {
                'productCode': product_code,
                'reqNo': req_no,
                'queryData': response_array
            }
            self.reponse = resp
            logger.info("5- 》》》》》》》》》》》》》》》》》》》》》》》》》流程结束 pipes 回调 defensor 《《《《《《《《《《《《《《《《《《《《《《《《《《《")
            logger.info("5-1 response》》》》"+ str(self.reponse))
            return self.reponse
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def strategy_process(self):
        # 获取请求参数
        try:
            json_data = request.get_json()
            logger.info("1- 》》》》》》》》》》》》》》》》》》》一级联合报告 defensor invoke pipes 获取策略引擎结果，流程开启《《《《《《《《《《《《《《《《《《《《《《《")
            logger.debug("1-1 json_data》》》》》"+str(json.dumps(json_data)))
            strategy_param = json_data.get('strategyParam')
            req_no = strategy_param.get('reqNo')
            product_code = strategy_param.get('productCode')
            step_req_no = strategy_param.get('stepReqNo')
            versionNo = strategy_param.get('versionNo')
            query_data_array = strategy_param.get('queryData')
            subject = []
            cache_arry = []
            # 遍历query_data_array调用strategy
            for data in query_data_array:
                array, resp = self._strategy_second_hand(data, product_code, req_no)
                subject.append(resp)
                cache_arry.append(array)
            # 封装第二次调用参数
            logger.info("5- 》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》开始封装第二次调用策略引擎入参《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《")
            variables = self._create_strategy_second_request(cache_arry)
            strategy_request = _build_request(req_no, product_code, variables=variables)
            logger.info("5-1 strategy_request》》》》"+str(strategy_request))
            logger.info("6 》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》开始第二次调用策略引擎《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《")
            strategy_response = requests.post(STRATEGY_URL, json=strategy_request)
            if strategy_response.status_code != 200:
                raise Exception("strategyOne错误:" + strategy_response.text)
            strategy_resp = strategy_response.json()
            logger.info("7- 》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》第二次调用策略引擎成功《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《")
            logger.info("7-1 strategy_resp》》》》"+str(strategy_resp))
            error = jsonpath(strategy_resp, '$..Error')
            if error:
                raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(strategy_resp, '$..Description')))
            score_to_int(strategy_resp)
            # 封装最终返回json
            resp_end = self._create_strategy_resp(product_code, req_no, step_req_no, strategy_resp, variables,
                                                  versionNo, subject)
            self.reponse = resp_end
            logger.info("8- 》》》》》》》》》》》》》》》》》》》》》》》》》流程结束 pipes 回调 defensor 《《《《《《《《《《《《《《《《《《《《《《《《《《《")
            logger.info("8-1 response》》》》" + str(self.reponse))
            return self.reponse
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))


    def _create_strategy_resp(self, product_code, req_no, step_req_no, strategy_resp, variables, versionNo, subject):
        resp = {}
        resp['reqNo'] = req_no
        resp['product_code'] = product_code
        resp['stepReqNo'] = step_req_no
        resp['versionNo'] = versionNo
        resp['strategyInputVariables'] = variables
        resp['strategyResult'] = strategy_resp
        resp['subject'] = subject
        return resp

    def _create_strategy_second_request(self, cache_arry):
        """
        挑选最多10个个人主体和10个企业主体封装入参
        :param cache_arry:
        :return:
        """
        df = pd.DataFrame(cache_arry)
        if df.query('relation == "MAIN" and userType == "PERSONAL"').shape[0] > 0:
            # 借款主体为个人-联合报告-排序
            self._sort_union_person_df(df)
        elif df.query('relation == "MAIN" and userType == "COMPANY"').shape[0] > 0:
            # 借款主体为企业-联合报告-排序
            self._sort_union_company_df(df)
        else:
            raise ServerException(code=500, description=str('没有借款主体'))
        # 取前10行数据
        df_person = df.query('userType=="PERSONAL" and order != 999').sort_values(by=["fundratio"],
                                                                                  ascending=False).sort_values(
            by=["order"], ascending=True)[0:10]
        df_compay = df.query('userType=="COMPANY" and order != 999').sort_values(by=["fundratio"],
                                                                                 ascending=False).sort_values(
            by=["order"], ascending=True)[0:10]
        # 拼接入参variables
        variables = self._strategy_second_request_variables(df_compay, df_person)
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

    def _sort_union_company_df(self, df):
        for index, row in df.iterrows():
            if row['relation'] == 'CONTROLLER' and row['userType'] == 'PERSONAL':
                df.loc[index, 'order'] = 0
            elif row['relation'] == 'CONTROLLER_SPOUSE' and row['userType'] == 'PERSONAL':
                df.loc[index, 'order'] = 1
            elif row['relation'] == 'SHAREHOLDER' and row['userType'] == 'PERSONAL' and row['fundratio'] >= 0.50:
                df.loc[index, 'order'] = 2
            elif row['relation'] == 'LEGAL' and row['userType'] == 'PERSONAL':
                df.loc[index, 'order'] = 3
            elif row['relation'] == 'SHAREHOLDER' and row['userType'] == 'PERSONAL' and row['fundratio'] < 0.50:
                df.loc[index, 'order'] = 4
            elif row['relation'] == 'OTHER' and row['userType'] == 'PERSONAL':
                df.loc[index, 'order'] = 5
            elif row['relation'] == 'CONTROLLER' and row['userType'] == 'COMPANY':
                df.loc[index, 'order'] = 0
            elif row['relation'] == 'SHAREHOLDER' and row['userType'] == 'COMPANY' and row['fundratio'] >= 0.50:
                df.loc[index, 'order'] = 1
            else:
                df.loc[index, 'order'] = 999

    def _sort_union_person_df(self, df):
        for index, row in df.iterrows():
            if row['relation'] == 'MAIN' and row['userType'] == 'PERSONAL':
                df.loc[index, 'order'] = 0
            elif row['relation'] == 'SPOUSE' and row['userType'] == 'PERSONAL':
                df.loc[index, 'order'] = 1
            elif row['relation'] == 'CHILDREN' and row['userType'] == 'PERSONAL':
                df.loc[index, 'order'] = 2
            elif row['relation'] == 'PARENT' and row['userType'] == 'PERSONAL':
                df.loc[index, 'order'] = 3
            elif row['relation'] == 'PARTNER' and row['userType'] == 'PERSONAL':
                df.loc[index, 'order'] = 4
            elif row['relation'] == 'OTHER' and row['userType'] == 'PERSONAL':
                df.loc[index, 'order'] = 5
            elif row['relation'] == 'CONTROLLER' and row['userType'] == 'COMPANY':
                df.loc[index, 'order'] = 0
            elif row['relation'] == 'SHAREHOLDER' and row['userType'] == 'COMPANY' and row['fundratio'] >= 0.50:
                df.loc[index, 'order'] = 1
            elif row['relation'] == 'LEGAL' and row['userType'] == 'COMPANY':
                df.loc[index, 'order'] = 2
            elif row['relation'] == 'SHAREHOLDER' and row['userType'] == 'COMPANY' and row['fundratio'] < 0.50:
                df.loc[index, 'order'] = 4
            else:
                df.loc[index, 'order'] = 999

    def _strategy_second_hand(self, data, product_code, req_no):
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
        variables, out_decision_code = translate_for_strategy(biz_types, user_name, id_card_no, phone,
                                                              user_type, base_type)
        origin_input['out_strategyBranch'] = ','.join(codes)
        # 合并新的转换变量
        origin_input.update(variables)
        logger.info("2- 》》》》》》》》》》》》》》》》》》》》》》》》》》开始策略引擎封装入参《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《")
        strategy_request = _build_request(req_no, product_code, origin_input)
        logger.info("2-1 strategy_request》》》》" + str(strategy_request))
        logger.info("3 》》》》》》》》》》》》》》》》》》》》》》》》》》开始调用策略引擎《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《")
        strategy_response = requests.post(STRATEGY_URL, json=strategy_request)
        logger.debug(strategy_response)
        if strategy_response.status_code != 200:
            raise Exception("strategyOne错误:" + strategy_response.text)
        strategy_resp = strategy_response.json()
        logger.info("4- 》》》》》》》》》》》》》》》》》》》》》》》》》》策略引擎调用成功《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《")
        logger.info("4-1 strategy_resp》》》》》"+str(strategy_resp))
        error = jsonpath(strategy_resp, '$..Error')
        if error:
            raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(strategy_resp, '$..Description')))
        score_to_int(strategy_resp)
        biz_types = _get_biz_types(strategy_resp)
        logger.info(biz_types)
        self._strategy_second_loop_resp(base_type, biz_types, data, id_card_no, out_decision_code, phone, product_code,
                                        resp, strategy_resp, user_name, user_type, variables)
        self._get_strategy_second_array(array, fundratio, relation, strategy_resp, user_name, user_type, variables)
        return array, resp

    def _get_strategy_second_array(self, array, fundratio, relation, strategy_resp, user_name, user_type, variables):
        array['name'] = user_name
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
        if STRATEGE_DONE in biz_types and base_type in ['U_PERSONAL', 'G_PERSONAL']:
            detail = translate_for_report_detail(product_code, user_name, id_card_no, phone, user_type,
                                                 base_type)
            resp['reportDetail'] = [detail]
        # 处理关联人
        _relation_risk_subject(strategy_resp, out_decision_code)
        resp['strategyResult'] = strategy_resp
        resp['rules'] = _append_rules(biz_types)
        resp['queryData'] = data

    def _shack_hander_response(self, data, product_code, req_no):
        """
        和决策交互，封装response
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
        fundratio = data.get('fundratio')
        relation = data.get('relation')
        # 获取base_type
        base_type = self._get_base_type(fundratio, auth_status, phone, relation, user_type)
        variables = T00000().run(user_name, id_card_no, phone, user_type, base_type)['variables']
        # 决策要求一直要加上00000，用户基础信息。
        variables['out_strategyBranch'] = '00000'
        logger.info("2- 》》》》》》》》》》》》》》》》》》》》》》》》》》开始策略引擎封装入参《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《")
        strategy_request = _build_request(req_no, product_code, variables=variables)
        logger.info("2-1 strategy_request》》》》"+str(strategy_request))
        # 调用决策引擎
        logger.info("3- 》》》》》》》》》》》》》》》》》》》》》》》》》》开始调用策略引擎《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《")
        response = requests.post(STRATEGY_URL, json=strategy_request)
        if response.status_code != 200:
            raise Exception("strategyOne错误:" + response.text)
        resp_json = response.json()
        logger.info("4- 》》》》》》》》》》》》》》》》》》》》》》》》》》》策略引擎调用成功《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《《")
        logger.info("4-1 resp_json》》》》"+str(resp_json))
        error = jsonpath(resp_json, '$..Error')
        if error:
            raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(resp_json, '$..Description')))
        biz_types = _get_biz_types(resp_json)
        rules = _append_rules(biz_types)
        resp['name'] = user_name
        resp['idno'] = id_card_no
        resp['phone'] = phone
        resp['userType'] = user_type
        resp['authStatus'] = auth_status
        resp['fundratio'] = fundratio
        resp['baseType'] = base_type
        resp['relation'] = relation
        resp['bizType'] = biz_types
        resp['rules'] = rules
        return resp

    def _get_base_type(self, fundratio, auth_status, phone, relation, user_type):
        """
        封装base_type
        :param fundratio:
        :param auth_status:
        :param phone:
        :param relation:
        :param user_type:
        :return: base_type
        """
        if relation == 'GUARANTOR':
            if user_type == 'COMPANY':
                return 'G_COMPANY'
            elif auth_status == 'AUTHORIZED':
                return 'G_PERSONAL'
            else:
                return 'G_S_PERSONAL'
        if user_type == 'PERSONAL':
            if auth_status == 'AUTHORIZED' and phone is not None and phone != '':
                return 'U_PERSONAL'
            else:
                return 'U_S_PERSONAL'
        if user_type == 'COMPANY':
            if relation == 'CONTROLLER':
                return 'U_COMPANY'
            elif relation == 'LEGAL':
                return 'U_COMPANY'
            elif relation == 'SHAREHOLDER' and float(fundratio) >= 0.50:
                return 'U_COMPANY'
            else:
                return 'U_S_COMPANY'
