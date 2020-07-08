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
                "id":11879,
                "parentId":0,
                "name":"韩骁頔",
                "idno":"31011519910503253X",
                "phone":"13611647802",
                "userType":"PERSONAL",
                "authorStatus":"AUTHORIZED",
                "fundratio":0,
                "applyAmo":66600,
                "relation":"CONTROLLER",
                "extraParam":{
                    "bankName":"银行名",
                    "bankAccount":"银行账户",
                    "totalSalesLastYear":23232,
                    "industry":"E20",
                    "industryName":"xx行业",
                    "seasonable":"1",
                    "seasonOffMonth":"2,3",
                    "seasonOnMonth":"9,10"
                }
            },
            {
                "id":11880,
                "parentId":0,
                "name":"磁石供应链商业保理（深圳）有限公司",
                "idno":"91440300MA5EEJUR92",
                "phone":"021-1234567",
                "userType":"COMPANY",
                "fundratio":0,
                "applyAmo":66600,
                "relation":"MAIN",
                "extraParam":{
                    "bankName":"银行名",
                    "bankAccount":"银行账户",
                    "totalSalesLastYear":23232,
                    "industry":"E20",
                    "industryName":"xx行业",
                    "seasonable":"1",
                    "seasonOffMonth":"2,3",
                    "seasonOnMonth":"9,10"
                }
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
