# @Time : 2020/6/19 2:50 PM 
# @Author : lixiaobo
# @File : union_portrait.py 
# @Software: PyCharm

from portrait.portrait_processor import PortraitProcessor


class UnionPortrait(PortraitProcessor):
    """
    联合画像清洗
    """
    def __init__(self):
        super().__init__()

    def process(self):
        """
        参数query_data_array的结构如下：
        [
            {
                "applyAmo":66600,
                "authorStatus":"AUTHORIZED",
                "extraParam":{
                    "bankAccount":"银行账户",
                    "bankName":"银行名",
                    "industry":"E20",
                    "industryName":"xx行业",
                    "seasonOffMonth":"2,3",
                    "seasonOnMonth":"9,10",
                    "seasonable":"1",
                    "totalSalesLastYear":23232
                },
                "fundratio":0,
                "id":11879,
                "idno":"31011519910503253X",
                "name":"韩骁頔",
                "parentId":0,
                "phone":"13611647802",
                "relation":"CONTROLLER",
                "userType":"PERSONAL",
                "baseType":"U_COM_CT_PERSONAL"
            },
            {
                "applyAmo":66600,
                "extraParam":{
                    "bankAccount":"银行账户",
                    "bankName":"银行名",
                    "industry":"E20",
                    "industryName":"xx行业",
                    "seasonOffMonth":"2,3",
                    "seasonOnMonth":"9,10",
                    "seasonable":"1",
                    "totalSalesLastYear":23232
                },
                "fundratio":0,
                "id":11880,
                "idno":"91440300MA5EEJUR92",
                "name":"磁石供应链商业保理（深圳）有限公司",
                "parentId":0,
                "phone":"021-1234567",
                "relation":"MAIN",
                "userType":"COMPANY",
                "baseType":"U_COMPANY"
            }
        ]

        self.public_param的参数包含：
            "reqNo"
            "reportReqNo"
            "productCode"
            "isSingle"
            "outApplyNo"
            "applyAmt"
            "renewLoans"
            "historicalBiz"
        """
        pass
