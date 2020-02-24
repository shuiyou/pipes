# -*- coding: utf-8 -*-
# @Time : 2020/2/24 9:20 AM
# @Author : lixiaobo
# @Site : 
# @File : t16001.py
# @Software: PyCharm
from mapping.tranformer import Transformer
from mapping.utils.df_comparator_util import df_compare
from util.mysql_reader import sql_to_df


class T16001(Transformer):
    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'court_owed_owe_laf': '0',  # 法院核查_个人_欠款欠费名单贷后新增
            'court_dishonesty_laf': '0',  # 法院核查_个人_失信老赖名单_贷后新增
            'court_limit_entry_laf': '0',  # 法院核查_个人_限制出入境名单_贷后新增
            'court_high_cons_laf': '0',  # 法院核查_个人_限制高消费名单命中次数_贷后新增
            'court_cri_sus_laf': '0',  # 法院核查_企业_罪犯及嫌疑人名单命中次数_贷后新增
            'court_fin_loan_con_laf': '0',  # 法院核查_个人_金融借款合同纠纷_贷后新增
            'court_loan_con_laf': '0',  # 法院核查_个人_借款合同纠纷_贷后新增
            'court_pop_loan_laf': '0',  # 法院核查_个人_民间借贷纠纷_贷后新增
            'court_admi_vio_laf': '0',  # 法院核查_个人_行政违法记录命中次数_贷后新增
            'court_judge_laf': '0',  # 法院核查_个人_民商事裁判文书命中次数_贷后新增
            'court_trial_proc_laf': '0',  # 法院核查_个人_民商事审判流程命中次数_贷后新增
            'court_tax_pay_laf': '0',  # 法院核查_个人_纳税非正常户命中次数_贷后新增
            'court_pub_info_laf': '0',  # 法院核查_个人_执行公开信息命中次数_贷后新增
            'court_tax_arrears_laf': '0',  # 法院核查_个人_欠税名单命中次数_贷后新增
        }
        self.pre_biz_date = None

    def _court_owed_owe_laf(self, variable_name):
        old_sql = '''
        select contract_no from(
            select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s 
            and create_time < %(pre_biz_date)s order by create_time desc limit 1
            ) tab left join info_court_arrearage arr on arr.court_id = tab.id where contract_no is not null;
        '''
        new_sql = '''
        select contract_no from(
            select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) 
            ) tab left join info_court_arrearage arr on arr.court_id = tab.id where contract_no is not null;
        '''
        old_df = sql_to_df(sql=old_sql,
                           params={"user_name": self.user_name,
                                   "id_card_no": self.id_card_no,
                                   "pre_biz_date": self.pre_biz_date})
        new_df = sql_to_df(sql=new_sql,
                           params={"user_name": self.user_name,
                                   "id_card_no": self.id_card_no})

        df_compare(self.variables, old_df, new_df, variable_name)

    def transform(self):
        # 前一个业务的创建时间
        self.pre_biz_date = self.origin_data.get('preBizDate')

        # 法院核查_个人_欠款欠费名单贷后新增
        self._court_owed_owe_laf("court_owed_owe_laf")
