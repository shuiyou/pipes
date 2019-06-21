from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer
import pandas as pd
import json

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

## 网申核查相关的变量模块
class T17001(Transformer):

    def get_biz_type(self):
        """
        返回这个转换对应的biz type
        :return:
        """
        return ''

    def __init__(self) -> None:

        super().__init__()
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
            'td_risk_idc_hit_ovdu': 0,  # 网申核查_身份证命中信贷逾期名单
            'td_risk_idc_hit_court_dish': 0,  # 网申核查_身份证命中法院失信名单
            'td_risk_idc_hit_high_att': 0,  # 网申核查_身份证命中高风险关注名单
            'td_bah_1d_dev_rel_tel': 0,  # 网申核查_1天内设备关联手机号数
            'td_bah_1d_dev_rel_idc': 0,  # 网申核查_1天内设备关联身份证数
            'td_bah_1d_idc_rel_dev': 0,  # 网申核查_1天内身份证关联设备数
            'td_bah_1d_tel_rel_dev': 0,  # 网申核查_1天内手机号关联设备数
            'td_bah_7d_dev_app': 0,  # 网申核查_7天内设备申请次数
            'td_bah_7d_idc_app': 0,  # 网申核查_7天内身份证申请次数
            'td_bah_7d_tel_app': 0,  # 网申核查_7天内手机号申请次数
            'td_bah_7d_dev_rel_idc': 0,  # 网申核查_7天内设备关联身份证数
            'td_bah_7d_dev_rel_tel': 0,  # 网申核查_7天内设备关联手机号数
            'td_bah_7d_idc_rel_dev': 0,  # 网申核查_7天内身份证关联设备数
            'td_bah_7d_tel_rel_dev': 0,  # 网申核查_7天内手机号关联设备数
            'td_bah_1m_dev_app': 0,  # 网申核查_1个月内设备申请次数
            'td_bah_1m_idc_app': 0,  # 网申核查_1个月内身份证申请次数
            'td_bah_1m_tel_app': 0,  # 网申核查_1个月内手机号申请次数
            'td_bah_1m_idc_rel_dev': 0,  # 网申核查_1个月内身份证关联设备数
            'td_bah_3m_add_rel_idc': 0,  # 网申核查_3个月家庭地址关联身份证数
            'td_bah_3m_bcname_rel_idc': 0,  # 网申核查_3个月内银行卡_姓名关联多个身份证
            'td_bah_3m_idc_rel_add': 0,  # 网申核查_3个月身份证关联家庭地址数
            'td_bah_3m_idc_rel_tel': 0,  # 网申核查_3个月身份证关联手机号数
            'td_bah_3m_idc_rel_bctel': 0,  # 网申核查_3个月身份证关联银行卡预留手机号数
            'td_bah_3m_idc_rel_mail': 0,  # 网申核查_3个月身份证关联邮箱数
            'td_bah_3m_tel_rel_bctel': 0,  # 网申核查_3个月手机号关联银行卡预留手机号数
            'td_bah_3m_mail_rel_idc': 0,  # 网申核查_3个月邮箱关联身份证数
            'td_bah_3m_tel_rel_idc': 0,  # 网申核查_3个月手机号码关联身份证数
            'td_apply_7d': 0,  # 网申核查_7天内申请人在多个平台申请借款
            'td_apply_1m': 0,  # 网申核查_1个月内申请人在多个平台申请借款
            'td_apply_3m': 0,  # 网申核查_3个月内申请人在多个平台申请借款
            'td_apply_6m': 0,  # 网申核查_6个月内申请人在多个平台申请借款
            'td_apply_12m': 0,  # 网申核查_12个月内申请人在多个平台申请借款
            'td_risk_age_high': 0,  # 网申核查_申请人属于高风险年龄段人群
            'td_idc_name_hit_dish_vague': 0,  # 网申核查_身份证_姓名命中法院失信模糊名单
            'td_idc_name_hit_exec_vague': 0,  # 网申核查_身份证_姓名命中法院执行模糊名单
            'td_applicant_idc_3m_morethan2': 0,  # 网申核查_3个月内申请人身份证作为联系人身份证出现的次数大于等于2
            'td_applicant_tel_3m_morethan2': 0,  # 网申核查_3个月内申请人手机号作为联系人手机号出现的次数大于等于2

    # 获取目标数据集
    def _info_fraud_verification_item(self):

        sql = '''
               SELECT * FROM info_fraud_verification_item  WHERE  fraud_verification_id 
               IN (SELECT fv.fraud_verification_id FROM (SELECT id fraud_verification_id FROM info_fraud_verification 
               WHERE user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s
               ORDER BY id DESC LIMIT 1) as fv);
        '''
        df = sql_to_df(sql=(sql),params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    # 计算个人基本信息核查模块字段
    def _per_base_info(self, df=None):

        if len(df[df['item_group'] == '个人基本信息核查']) != 0:
            df_1 = df[df['item_name'].str.contains('手机号命中通信小号库')]
            self.variables['td_tel_small'] = len(df_1)
            df_2 = df[df['item_name'].str.contains('手机号命中虚假号码库')]
            self.variables['td_tel_virtual'] = len(df_2)
            df_3 = df[df['item_name'].str.contains('手机号命中诈骗骚扰库')]
            self.variables['td_tel_fraud'] = len(df_3)
            df_4 = df[df['item_name'].str.contains('申请人属于高风险年龄段人群')]
            self.variables['td_risk_age_high'] = len(df_4)

    # 计算风险信息扫描模块字段
    def _risk_info(self, df=None):

        if len(df[df['item_group'] == '风险信息扫描']) != 0:
            df_1 = df[df['item_name'].str.contains('手机号命中中风险关注名单')]
            self.variables['td_tel_risk_m'] = len(df_1)
            df_2 = df[df['item_name'].str.contains('手机号命中低风险关注名单')]
            self.variables['td_tel_risk_l'] = len(df_2)
            df_3 = df[df['item_name'].str.contains('手机号命中车辆租赁违约名单')]
            self.variables['td_tel_veh'] = len(df_3)
            df_4 = df[df['item_name'].str.contains('手机号命中欠款公司法人代表名单')]
            self.variables['td_tel_debt'] = len(df_4)
            df_5 = df[df['item_name'].str.contains('手机号命中信贷逾期后还款名单')]
            self.variables['td_tel_repay'] = len(df_5)
            df_6 = df[df['item_name'].str.contains('身份证命中犯罪通缉名单')]
            self.variables['td_idno_crime'] = len(df_6)
            df_7 = df[df['item_name'].str.contains('身份证命中法院执行名单')]
            self.variables['td_idno_exec'] = len(df_7)
            df_8 = df[df['item_name'].str.contains('身份证命中法院结案名单')]
            self.variables['td_idno_end'] = len(df_8)
            df_9 = df[df['item_name'].str.contains('身份证命中车辆租赁违约名单')]
            self.variables['td_idno_veh'] = len(df_9)
            df_10 = df[df['item_name'].str.contains('身份证命中中风险关注名单')]
            self.variables['td_idno_risk_m'] = len(df_10)
            df_11 = df[df['item_name'].str.contains('身份证命中低风险关注名单')]
            self.variables['td_idno_risk_l'] = len(df_11)
            df_12 = df[df['item_name'].str.contains('身份证命中欠款公司法人代表名单')]
            self.variables['td_idno_debt'] = len(df_12)
            df_13 = df[df['item_name'].str.contains('身份证命中欠税名单')]
            self.variables['td_idno_tax'] = len(df_13)
            df_14 = df[df['item_name'].str.contains('身份证命中欠税公司法人代表名单')]
            self.variables['td_idno_tax_rep'] = len(df_14)
            df_15 = df[df['item_name'].str.contains('身份证命中信贷逾期后还款名单')]
            self.variables['td_idno_repay'] = len(df_15)
            df_16 = df[df['item_name'].str.contains('手机号命中信贷逾期名单')]
            self.variables['td_risk_tel_hit_ovdu'] = len(df_16)
            df_17 = df[df['item_name'].str.contains('手机号命中高风险关注名单')]
            self.variables['td_risk_tel_hit_high_att'] = len(df_17)
            df_18 = df[df['item_name'].str.contains('身份证命中信贷逾期名单')]
            self.variables['td_risk_idc_hit_ovdu'] = len(df_18)
            df_19 = df[df['item_name'].str.contains('身份证命中法院失信名单')]
            self.variables['td_risk_idc_hit_court_dish'] = len(df_19)
            df_20= df[df['item_name'].str.contains('身份证命中高风险关注名单')]
            self.variables['td_risk_idc_hit_high_att'] = len(df_20)
            df_21 = df[df['item_name'].str.contains('身份证_姓名命中法院失信模糊名单')]
            self.variables['td_idc_name_hit_dish_vague'] = len(df_21)
            df_22 = df[df['item_name'].str.contains('身份证_姓名命中法院执行模糊名单')]
            self.variables['td_idc_name_hit_exec_vague'] = len(df_22)

    # 计算客户行为检测模块字段
    def _cus_behav(self, df=None):

        if len(df[df['item_group'] == '客户行为检测']) != 0:
            df1  = df.loc[(df['item_group'] == '客户行为检测') & (df['item_detail'] != ''),:].copy()
            row_list = []
            for index, col in df1.iterrows():
                row_str = dict(col).get('item_detail')
                row_dict = json.loads(row_str)
                row_list.append(row_dict)
            df2 = pd.DataFrame(row_list)
            new_lst = []
            for _, row in df2.iterrows():
                new_dct = dict()
                for i in range(len(row.frequency_detail_list)):
                    new_dct[row.frequency_detail_list[i].get('detail').split('：')[0]] = \
                    row.frequency_detail_list[i].get('detail').split('：')[1]
                new_lst.append(new_dct)
            df3 = pd.DataFrame(new_lst)


