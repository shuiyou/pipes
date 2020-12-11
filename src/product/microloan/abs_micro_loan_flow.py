# @Time : 12/11/20 10:12 AM 
# @Author : lixiaobo
# @File : micro_loan_flowable.py 
# @Software: PyCharm

import pandas as pd
from jsonpath import jsonpath

from logger.logger_util import LoggerUtil
from mapping.grouped_tranformer import invoke_each
from mapping.mapper import translate_for_strategy
from product.p_utils import score_to_int, _get_biz_types, _relation_risk_subject, _append_rules, \
    _get_resp_field_value
from util.strategy_invoker import invoke_strategy
from view.grouped_mapper_detail import view_variables_scheduler
from view.mapper_detail import STRATEGE_DONE

logger = LoggerUtil().logger(__name__)


class MicroLoanFlow(object):
    def __init__(self, json_data):
        self.sql_db = None
        self.df_client = None
        self.json_data = json_data
        self.strategy_param = self.json_data.get('strategyParam')
        self.req_no = self.strategy_param.get('reqNo')
        self.step_req_no = self.strategy_param.get('stepReqNo')
        self.version_no = self.strategy_param.get('versionNo')
        self.product_code = self.strategy_param.get('productCode')
        self.query_data_array = self.strategy_param.get('queryData')

    @staticmethod
    def _create_strategy_resp(strategy_resp, variables, common_detail, subject, json_data):
        passthrough_msg = json_data.get("passthroughMsg")
        resp = {
            'passthroughMsg': passthrough_msg,
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
            if row["phantomRelation"]:
                continue

            person_index = person_index + 1
            variables['score_p' + str(person_index)] = row['score']
            variables['score_fraud_p' + str(person_index)] = row['score_fraud']
            variables['score_owner_p' + str(person_index)] = row['score_owner']
            variables['score_bus_p' + str(person_index)] = row['score_bus']
            variables['score_black_p' + str(person_index)] = row['score_black']
            variables['score_fin_p' + str(person_index)] = row['score_fin']
            variables['model_pred_p' + str(person_index)] = row['model_pred']
            variables['td_pred_p' + str(person_index)] = row['td_pred']
        # 公司作为入参参与报告分
        df_company = df.query('userType=="COMPANY" and strategy=="01"')
        logger.info("-------df_company\n%s", df_company)
        company_index = 0
        for index, row in df_company.iterrows():
            if row["phantomRelation"] or row['score'] <= 0:
                continue

            company_index = company_index + 1
            variables['score_c' + str(company_index)] = row['score']
            variables['model_pred_c' + str(company_index)] = row['model_pred']
        variables['base_type'] = 'UNION'
        return variables

    def _strategy_hand(self, json_data, data, product_code, req_no):
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
        origin_input['out_strategyBranch'] = ','.join(filter(lambda e: e != "00000", codes))
        # 合并新的转换变量
        origin_input.update(variables)
        origin_input["segment_name"] = data.get("nextSegmentName")

        strategy_resp = invoke_strategy(origin_input, product_code, req_no)
        score_to_int(strategy_resp)
        biz_types, categories = _get_biz_types(strategy_resp)
        segment_name = _get_resp_field_value(strategy_resp, "$..segment_name")
        data["segmentName"] = data.get("nextSegmentName")
        data["nextSegmentName"] = segment_name
        data["bizType"] = biz_types

        resp = {}
        self._calc_view_variables(base_type, biz_types, json_data, data, id_card_no, out_decision_code, phone,
                                  product_code,
                                  resp, strategy_resp, user_name, user_type, variables)
        array = self._get_strategy_second_array(data, fund_ratio, relation, strategy_resp, user_name, user_type,
                                                variables)
        return array, resp


    def _get_strategy_second_array(self, data, fundratio, relation, strategy_resp, user_name, user_type,
                                   variables):
        array = {
            'name': user_name,
            'idno': data.get('idno'),
            'userType': user_type,
            'strategy': data.get("extraParam").get("strategy"),
            'phantomRelation': data.get("extraParam").get("phantomRelation")
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
        array['td_pred'] = self._get_json_path_value(strategy_resp, '$..td_pred')
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
    def _calc_view_variables(base_type, biz_types, json_data, data, id_card_no, out_decision_code, phone, product_code,
                             resp, strategy_resp, user_name, user_type, variables):
        """
        每次循环后封装每个主体的resp信息
        """
        data['strategyInputVariables'] = variables
        if STRATEGE_DONE in biz_types:
            detail = view_variables_scheduler(product_code, json_data, user_name, id_card_no, phone, user_type,
                                              base_type,
                                              data, invoke_each)
            resp['reportDetail'] = detail
        # 处理关联人
        _relation_risk_subject(strategy_resp, out_decision_code)
        resp['strategyResult'] = strategy_resp
        resp['rules'] = _append_rules(biz_types)
        resp['queryData'] = data