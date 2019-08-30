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
from util.common_util import exception
from view.mapper_detail import STRATEGE_DONE, translate_for_report_detail

logger = LoggerUtil().logger(__name__)


class P003(Generate):

    @exception('purpose= 联合报告shack_hander_process&author=liujinhao')
    def shack_hander_process(self):
        try:
            json_data = request.get_json()
            logger.debug(json.dumps(json_data))
            req_no = json_data.get('reqNo')
            product_code = json_data.get('productCode')
            query_data_array = json_data.get('queryData')
            response_array = []
            #遍历query_data_array调用strategy
            for data in query_data_array:
                response_array.append(self.shack_hander_response(data, product_code, req_no))
            resp = {
                'productCode': product_code,
                'reqNo': req_no,
                'queryData':response_array
            }
            return jsonify(resp)
        except Exception as err:
            logger.error(str(err))
            raise ServerException(code=500, description=str(err))

    @exception('purpose= 联合报告strategy_process&author=liujinhao')
    def strategy_process(self):
        try:
            # 获取请求参数
            resp = {}
            json_data = request.get_json()
            logger.debug(json.dumps(json_data))
            strategy_param = json_data.get('strategyParam')
            req_no = strategy_param.get('reqNo')
            product_code = strategy_param.get('productCode')
            query_data_array = json_data.get('queryData')
            subject = []
            cache_arry = []
            # 遍历query_data_array调用strategy
            for data in query_data_array:
                array, resp = self.strategy_second_hand(data, product_code, req_no)
                subject.append(resp)
                cache_arry.append(array)
            #封装第二次调用参数
            variables = self.create_strategy_second_request(cache_arry)
            strategy_request = _build_request(req_no, product_code, variables=variables)
            logger.info(strategy_request)
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
            #封装最终返回json

            return jsonify(resp)
        except Exception as err:
            logger.error(str(err))
            raise ServerException(code=500, description=str(err))

    def create_strategy_second_request(self, cache_arry):
        pass

    def strategy_second_hand(self, data, product_code, req_no):
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
        ralation = data.get('ralation')
        fundratio = data.get('fundratio')
        biz_types = codes.copy()
        biz_types.append('00000')
        variables, out_decision_code = translate_for_strategy(biz_types, user_name, id_card_no, phone,
                                                              user_type, base_type)
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
        self.strategy_second_loop_resp(base_type, biz_types, data, id_card_no, out_decision_code, phone, product_code,
                                       resp, strategy_resp, user_name, user_type, variables)
        self.get_strategy_second_array(array, fundratio, ralation, strategy_resp, user_name, user_type, variables)
        return array, resp

    def get_strategy_second_array(self, array, fundratio, ralation, strategy_resp, user_name, user_type, variables):
        array['name'] = user_name
        array['userType'] = user_type
        array['fundratio'] = fundratio
        array['ralation'] = ralation
        array['per_face_relent_indusCode1'] = self.get_json_path_value(variables, '$..per_face_relent_indusCode1')
        array['com_bus_face_outwardindusCode1'] = self.get_json_path_value(variables,
                                                                           '$..com_bus_face_outwardindusCode1')
        array['com_bus_industrycode'] = self.get_json_path_value(variables, '$..com_bus_industrycode')
        array['score_black'] = self.get_json_path_value(strategy_resp, '$..score_black')
        array['score_credit'] = self.get_json_path_value(strategy_resp, '$..score_credit')
        array['score_debit'] = self.get_json_path_value(strategy_resp, '$..score_debit')
        array['score_fraud'] = self.get_json_path_value(strategy_resp, '$..score_fraud')
        array['score_business'] = self.get_json_path_value(strategy_resp, '$..score_business')
        array['score'] = self.get_json_path_value(strategy_resp, '$..score')

    def get_json_path_value(self,strategy_resp,path):
        res = jsonpath(strategy_resp, path)
        if isinstance(res, list) and len(res) > 0:
            return res[0]
        else:
           return None

    def strategy_second_loop_resp(self, base_type, biz_types, data, id_card_no, out_decision_code, phone, product_code,
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
        if STRATEGE_DONE in biz_types and base_type in ['U_PERSONAL','G_PERSONAL']:
            detail = translate_for_report_detail(product_code, user_name, id_card_no, phone, user_type,
                                                 base_type)
            resp['reportDetail'] = [detail]
        # 处理关联人
        _relation_risk_subject(strategy_resp, out_decision_code)
        resp['strategyResult'] = strategy_resp
        resp['rules'] = _append_rules(biz_types)
        resp['queryData'] = data

    def shack_hander_response(self, data, product_code, req_no):
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
        ralation = data.get('ralation')
        # 获取base_type
        base_type =  self.get_base_type(fundratio, auth_status, phone, ralation, user_type)
        variables = T00000().run(user_name, id_card_no, phone, user_type,base_type)['variables']
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
        rules = _append_rules(biz_types)
        resp['name'] = user_name
        resp['idno'] = id_card_no
        resp['phone'] = phone
        resp['userType'] = user_type
        resp['authStatus'] = auth_status
        resp['fundratio'] = fundratio
        resp['baseType'] = base_type
        resp['ralation'] = ralation
        resp['bizType'] = biz_types
        resp['rules'] = rules
        return resp

    def get_base_type(self, fundratio, auth_status, phone, ralation, user_type):
        """
        封装base_type
        :param fundratio:
        :param auth_status:
        :param phone:
        :param ralation:
        :param user_type:
        :return: base_type
        """
        if ralation == 'GUARANTOR':
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
            if ralation == 'CONTROLLER':
                return 'U_COMPANY'
            elif ralation == 'LEGAL':
                return 'U_COMPANY'
            elif ralation == 'SHAREHOLDER' and float(fundratio) >= 0.50:
                return 'U_COMPANY'
            else:
                return 'U_S_COMPANY'





