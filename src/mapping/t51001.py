
# @Time : 2020/6/19 1:52 PM
# @Author : lixiaobo
# @File : t51001.py.py 
# @Software: PyCharm
from logger.logger_util import LoggerUtil
from mapping.p08001_m.app_amt_predication import ApplyAmtPrediction
from mapping.p08001_m.get_variable_in_db import GetVariableInDB
from mapping.p08001_m.get_variable_in_flow import GetVariableInFlow
from mapping.tranformer import Transformer

logger = LoggerUtil().logger(__name__)


class T51001(Transformer):
    """
    流水报告决策入参及变量清洗调度中心
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {

        }

    def transform(self):
        """
        input_param 为所有关联关系的入参
        [
            {
                "applyAmo":66600,
                "authorStatus":"AUTHORIZED",
                "extraParam":{
                    "bankName":"银行名",
                    "bankAccount":"银行账户",
                    "totalSalesLastYear":23232,
                    "industry":"E20",
                    "industryName":"xx行业",
                    "seasonable":"1",
                    "seasonOffMonth":"2,3",
                    "seasonOnMonth":"9,10"
                },
                "fundratio":0,
                "id":11843,
                "idno":"31011519910503253X",
                "name":"韩骁頔",
                "parentId":0,
                "phone":"13611647802",
                "relation":"CONTROLLER",
                "userType":"PERSONAL",
                "preReportReqNo":"PR472454663971700736",
                "baseTypeDetail":"U_COM_CT_PERSONAL"
            },
            {
                "applyAmo":66600,
                "extraParam":{
                    "bankName":"银行名",
                    "bankAccount":"银行账户",
                    "totalSalesLastYear":23232,
                    "industry":"E20",
                    "industryName":"xx行业",
                    "seasonable":"1",
                    "seasonOffMonth":"2,3",
                    "seasonOnMonth":"9,10"
                },
                "fundratio":0,
                "id":11844,
                "idno":"91440300MA5EEJUR92",
                "name":"磁石供应链商业保理（深圳）有限公司",
                "parentId":0,
                "phone":"021-1234567",
                "relation":"MAIN",
                "userType":"COMPANY",
                "preReportReqNo":"PR472454663971700736",
                "baseTypeDetail":"U_COMPANY"
            }
        ]
        """
        logger.info("input_param:%s", self.cached_data.get("input_param"))

        handle_list = [
            GetVariableInFlow(),
            GetVariableInDB(),
            ApplyAmtPrediction(),
        ]

        for handler in handle_list:
            handler.init(self.variables, self.user_name, self.id_card_no, self.origin_data, self.cached_data)
            handler.process()
