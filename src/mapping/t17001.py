from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

## 短信核查相关的变量模块
class T13001(Transformer):

    def get_biz_type(self):
        """
        返回这个转换对应的biz type
        :return:
        """
        return ''

    def __init__(self, user_name, id_card_no, phone) -> None:

        super().__init__()
        self.user_name = user_name
        self.id_card_no = id_card_no
        self.phone = phone
        self.variables = {
            'td_tel_small': 0,  # 网申核查_手机号命中通信小号库
            'td_tel_virtual': 0,  # 网申核查_手机号命中虚假号码库
            'td_tel_fraud': 0,  # 网申核查_手机号命中诈骗骚扰库
            'td_tel_risk_m': 0,  # 网申核查_手机号命中中风险关注名单
            'td_tel_risk_l': 0,  # 网申核查_手机号命中低风险关注名单
            'td_tel_veh': 0,  # 网申核查_手机号命中车辆租赁违约名单
            'td_tel_debt': 0, # 网申核查_手机号命中欠款公司法人代表名单
            'td_tel_repay': 0, # 网申核查_手机号命中信贷逾期后还款名单
            'td_idno_crime': 0, # 网申核查_身份证命中犯罪通缉名单
            'td_idno_exec': 0,  # 网申核查_身份证命中法院执行名单
            'td_idno_end': 0,  # 网申核查_身份证命中法院结案名单
            'td_idno_veh': 0,  # 网申核查_身份证命中车辆租赁违约名单
            'td_idno_risk_m': 0,  # 网申核查_身份证命中中风险关注名单
            'td_idno_risk_l': 0,  # 网申核查_身份证命中低风险关注名单
            'td_idno_debt': 0,  # 网申核查_身份证命中欠款公司法人代表名单

            'td_idno_tax': 0,  # 网申核查_身份证命中欠税名单
            'td_idno_tax_rep': 0,  # 网申核查_身份证命中欠税公司法人代表名单
            'td_idno_repay': 0,  # 网申核查_身份证命中信贷逾期后还款名单
            'td_risk_tel_hit_ovdu': 0,  # 网申核查_手机号命中信贷逾期名单
            'td_risk_tel_hit_high_att': 0,  # 网申核查_手机号命中高风险关注名单
            'td_tel_veh': 0,  # 网申核查_手机号命中车辆租赁违约名单
            'td_tel_debt': 0,  # 网申核查_手机号命中欠款公司法人代表名单
            'td_tel_repay': 0,  # 网申核查_手机号命中信贷逾期后还款名单
            'td_idno_crime': 0,  # 网申核查_身份证命中犯罪通缉名单
            'td_idno_exec': 0,  # 网申核查_身份证命中法院执行名单
            'td_idno_end': 0,  # 网申核查_身份证命中法院结案名单
            'td_idno_veh': 0,  # 网申核查_身份证命中车辆租赁违约名单
            'td_idno_risk_m': 0,  # 网申核查_身份证命中中风险关注名单
            'td_idno_risk_l': 0,  # 网申核查_身份证命中低风险关注名单
            'td_idno_debt': 0,  # 网申核查_身份证命中欠款公司法人代表名单






        }
