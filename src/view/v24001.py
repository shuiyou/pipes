import pandas as pd
from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer, subtract_datetime_col
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)



class V24001(Transformer):

    """
    工商核查相关的变量模块
    """

    def __init__(self) -> None:

        super().__init__()
        self.variables = {
            'com_bus_status': None,  # 工商核查_企业登记状态异常
            'com_bus_endtime': None,  # 工商核查_营业期限至
            'com_bus_relent_revoke': 0,  # 工商核查_关联公司吊销个数
            'com_bus_case_info': 0,  # 工商核查_现在是否有行政处罚信息
            'com_bus_shares_frost': 0,  # 工商核查_现在是否有股权冻结信息
            'com_bus_shares_frost_his': None,  # 工商核查_曾经是否有股权冻结信息
            'com_bus_shares_impawn': None,  # 工商核查_现在是否有股权出质登记信息
            'com_bus_shares_impawn_his': None  # 工商核查_曾经是否有股权出质登记信息

            'com_bus_mor_detail': 0,  # 工商核查_现在是否有动产抵押登记信息
            'com_bus_mor_detail_his': 0,  # 工商核查_曾经是否有动产抵押登记信息
            'com_bus_liquidation': 0,  # 工商核查_是否有清算信息
            'com_bus_exception': 0,  # 工商核查_现在是否有经营异常信息
            'com_bus_exception_his': None,  # 工商核查_曾经是否有经营异常信息
            'com_bus_illegal_list': None,  # 工商核查_现在是否有严重违法失信信息
            'com_bus_illegal_list_his': None,  # 工商核查_曾经是否有严重违法失信信息
            'com_bus_registered_capital': None  # 工商核查_注册资本（万元）

            'com_bus_openfrom': 0,  # 工商核查_营业期限自
            'com_bus_enttype': 0,  # 工商核查_类型
            'com_bus_esdate': 0,  # 工商核查_成立日期
            'com_bus_industryphycode': 0,  # 工商核查_行业门类代码
            'com_bus_areacode': None,  # 工商核查_住所所在行政区划代码
            'com_bus_industrycode': None,  # 工商核查_国民经济行业代码
            'com_bus_saicChanLegal_5y': None,  # 工商核查_法定代表人最近5年内变更次数
            'com_bus_saicChanInvestor_5y': None  # 工商核查_投资人最近5年内变更次数

            'com_bus_saicChanRegister_5y': 0,  # 工商核查_注册资本最近5年内变更次数
            'com_bus_saicAffiliated': 0,  # 工商核查_企业关联公司个数
            'com_bus_province': 0,  # 工商核查_省
            'com_bus_city': 0,  # 工商核查_市
            'com_bus_leg_not_shh': None,  # 工商核查_法人非股东
            'com_bus_exception_result': None,  # 工商核查_经营异常原因
            'com_bus_saicChanRunscope': None,  # 工商核查_经营范围变更次数
            'com_bus_legper_relent_revoke': None  # 工商核查_企业和法人关联公司是否存在吊销
            'com_bus_legper_outwardCount1': None  # 工商核查_企业、法人对外投资的公司数量




        }