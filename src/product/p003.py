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



class P003(Generate):

    def __init__(self)->None:
        super().__init__()
        self.reponse: {}

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
            self.reponse = resp
            return jsonify(self.reponse)
        except Exception as err:
            logger.error(str(err))
            raise ServerException(code=500, description=str(err))

    def shack_hander_response(self, data, product_code, req_no):
        queryParam = data.get_json()
        user_name = queryParam.get('name')
        id_card_no = queryParam.get('idno')
        phone = queryParam.get('phone')
        user_type = queryParam.get('userType')
        is_auth = queryParam.get('isAuth')
        fundratio = queryParam.get('fundratio')
        ralation = queryParam.get('ralation')
        base_type = ''
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
        rules = _append_rules(biz_types)
        resp = {
            'name': user_name,
            'idno': id_card_no,
            'phone': phone,
            'userType':user_type,
            'isAuth':is_auth,
            'fundratio':fundratio,
            'baseType':base_type,
            'ralation':ralation,
            'bizType':biz_types,
            'rules':rules
        }
        return jsonify(resp)

    def strategy_process(self):
        try:
            pass
        except Exception as err:
            logger.error(str(err))
            raise ServerException(code=500, description=str(err))

