# -*- coding: utf-8 -*-
# @Time : 2020/2/25 2:06 PM
# @Author : lixiaobo
# @Site : 
# @File : v16001.py.py
# @Software: PyCharm
import json

from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


class V16001(Transformer):
    def __init__(self) -> None:
        super().__init__()
        self.pre_biz_date = ""
        self.variables = {
            'info_court_owed_owe': None,  # 法院核查_个人_欠款欠费名单_贷前+贷后
            'info_court_dishonesty': None,  # 法院核查_个人_失信老赖名单_贷后+贷前
            'info_court_limit_entry': None,  # 法院核查_个人_限制出入境名单_贷后+贷前
            'info_court_high_cons': None,  # 法院核查_个人_限制高消费名单_贷后+贷前
            'info_court_cri_sus': None,  # 法院核查_企业_罪犯及嫌疑人名单_贷后+贷前
            'info_court_fin_loan_con': None,  # 法院核查_个人_金融借款合同纠纷_贷后+贷前
            'info_court_loan_con': None,  # 法院核查_个人_借款合同纠纷_贷后+贷前
            'info_court_pop_loan': None,  # 法院核查_个人_民间借贷纠纷_贷后+贷前
            'info_court_admi_vio': None,  # 法院核查_个人_行政违法记录_贷后+贷前
            'info_court_judge': None,  # 法院核查_个人_民商事裁判文书_贷后+贷前
            'info_court_trial_proc': None,  # 法院核查_个人_民商事审判流程_贷后+贷前
            'info_court_tax_pay': None,  # 法院核查_个人_纳税非正常户_贷后+贷前
            'info_court_pub_info': None,  # 法院核查_个人_执行公开信息_贷后+贷前
            'info_court_tax_arrears': None,  # 法院核查_个人_欠税名单_贷后+贷前
        }

    def _info_court_owed_owe(self, variable_name):
        old_sql = '''
                select t.* from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                    ) tab left join info_court_arrearage t on t.court_id = tab.id where contract_no is not null;
                '''
        new_sql = '''
                select t.* from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                    ) tab left join info_court_arrearage t on t.court_id = tab.id where contract_no is not null;
                '''
        self._sql_result_to_var(variable_name, "法院核查个人欠款欠费名单", old_sql, new_sql)

    def _info_court_dishonesty(self, variable_name):
        old_sql = '''
                select t.* from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                    ) tab left join info_court_deadbeat t on t.court_id = tab.id where execute_case_no is not null;
                '''
        new_sql = '''
                select t.* from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                    ) tab left join info_court_deadbeat t on t.court_id = tab.id where execute_case_no is not null;
                '''
        self._sql_result_to_var(variable_name, "法院核查个人失信老赖名单", old_sql, new_sql)

    def _info_court_limit_entry(self, variable_name):
        old_sql = '''
               select t.* from(
                   select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                   ) tab left join info_court_limited_entry_exit t on t.court_id = tab.id where execute_no is not null;
               '''
        new_sql = '''
               select t.* from(
                   select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                   ) tab left join info_court_limited_entry_exit t on t.court_id = tab.id where execute_no is not null;
               '''
        self._sql_result_to_var(variable_name, "法院核查个人限制出入境名单", old_sql, new_sql)

    def _info_court_high_cons(self, variable_name):
        old_sql = '''
                select t.* from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                    ) tab left join info_court_limit_hignspending t on t.court_id = tab.id where execute_case_no is not null;
                '''
        new_sql = '''
                select t.* from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                    ) tab left join info_court_limit_hignspending t on t.court_id = tab.id where execute_case_no is not null;
                '''
        self._sql_result_to_var(variable_name, "法院核查个人限制高消费名单", old_sql, new_sql)

    def _info_court_cri_sus(self, variable_name):
        old_sql = '''
               select t.* from(
                   select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                   ) tab left join info_court_criminal_suspect t on t.court_id = tab.id where case_no is not null;
               '''
        new_sql = '''
               select t.* from(
                   select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1 
                   ) tab left join info_court_criminal_suspect t on t.court_id = tab.id where case_no is not null;
               '''
        self._sql_result_to_var(variable_name, "法院核查企业罪犯及嫌疑人名单", old_sql, new_sql)

    def _court_fin_loan_stats(self, variable_name, type_name, case_reason):
        old_sql1 = '''
                select t.* from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                    ) tab left join info_court_judicative_pape t on t.court_id = tab.id where case_reason regexp %(case_reason)s and legal_status like "被告" and case_no is not null
                '''
        old_sql2 = '''
                select t.* from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                    ) tab left join info_court_trial_process t on t.court_id = tab.id where case_reason regexp %(case_reason)s and legal_status like "被告" and case_no is not null;
                '''

        new_sql1 = '''
                select t.* from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                    ) tab left join info_court_judicative_pape t on t.court_id = tab.id where case_reason regexp %(case_reason)s and legal_status like "被告" and case_no is not null
                '''

        new_sql2 = '''
                select t.* from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)  order by id desc limit 1
                    ) tab left join info_court_trial_process t on t.court_id = tab.id where case_reason regexp %(case_reason)s and legal_status like "被告" and case_no is not null;
                '''

        old_df1 = sql_to_df(sql=old_sql1, params={
            "user_name": self.user_name,
            "id_card_no": self.id_card_no,
            "pre_biz_date": self.pre_biz_date,
            "case_reason": case_reason
        })

        old_df2 = sql_to_df(sql=old_sql2, params={
            "user_name": self.user_name,
            "id_card_no": self.id_card_no,
            "pre_biz_date": self.pre_biz_date,
            "case_reason": case_reason
        })

        new_df1 = sql_to_df(sql=new_sql1, params={
            "user_name": self.user_name,
            "id_card_no": self.id_card_no,
            "pre_biz_date": self.pre_biz_date,
            "case_reason": case_reason
        })

        new_df2 = sql_to_df(sql=new_sql2, params={
            "user_name": self.user_name,
            "id_card_no": self.id_card_no,
            "pre_biz_date": self.pre_biz_date,
            "case_reason": case_reason
        })

        old_json = json.loads(old_df1.to_json(orient="records"))
        old_json1 = json.loads(old_df2.to_json(orient="records"))
        old_json.extend(old_json1)

        new_json = json.loads(new_df1.to_json(orient="records"))
        new1_json = json.loads(new_df2.to_json(orient="records"))
        new_json.extend(new1_json)

        print("new_json", new_json)
        print("old_json", old_json)

        self.variables[variable_name] = {"type:": type_name, "before": new_json,
                                         "after": old_json}

    def _info_court_admi_vio(self, variable_name):
        old_sql = '''
                    select t.* from(
                        select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                        ) tab left join  info_court_administrative_violation t on t.court_id = tab.id where case_no is not null;
                    '''
        new_sql = '''
                    select t.* from(
                        select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1 
                        ) tab left join info_court_administrative_violation t on t.court_id = tab.id where case_no is not null;
                    '''

        self._sql_result_to_var(variable_name, "法院核查个人行政违法记录", old_sql, new_sql)

    def _info_court_judge(self, variable_name):
        old_sql = '''
                select t.* from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                    ) tab left join  info_court_judicative_pape t on t.court_id = tab.id where case_no is not null;
                '''
        new_sql = '''
                select t.* from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                    ) tab left join info_court_judicative_pape t on t.court_id = tab.id where case_no is not null;
                '''
        self._sql_result_to_var(variable_name, "法院核查个人民商事裁判文书", old_sql, new_sql)

    def _info_court_trial_proc(self, variable_name):
        old_sql = '''
                    select t.* from(
                        select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                        ) tab left join  info_court_trial_process t on t.court_id = tab.id where case_no is not null;
                    '''
        new_sql = '''
                    select t.* from(
                        select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                        ) tab left join info_court_trial_process t on t.court_id = tab.id where case_no is not null;
                    '''
        self._sql_result_to_var(variable_name, "法院核查个人民商事审判流程", old_sql, new_sql)

    def _info_court_tax_pay(self, variable_name):
        old_sql = '''
                    select t.* from(
                        select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                        ) tab inner join  info_court_taxable_abnormal_user t on t.court_id = tab.id;
                    '''
        new_sql = '''
                    select t.* from(
                        select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                        ) tab inner join info_court_taxable_abnormal_user t on t.court_id = tab.id;
                    '''
        self._sql_result_to_var(variable_name, "法院核查个人纳税非正常户", old_sql, new_sql)

    def _info_court_pub_info(self, variable_name):
        old_sql = '''
                    select execute_case_no from(
                        select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                        ) tab left join  info_court_excute_public t on t.court_id = tab.id where execute_case_no is not null;
                    '''
        new_sql = '''
                    select execute_case_no from(
                        select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                        ) tab left join info_court_excute_public t on t.court_id = tab.id where execute_case_no is not null;
                    '''
        self._sql_result_to_var(variable_name, "法院核查_个人_执行公开信息", old_sql, new_sql)

    def _info_court_tax_arrears(self, variable_name):
        old_sql = '''
                    select t.* from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                    ) tab inner join  info_court_tax_arrears t on t.court_id = tab.id;
                  '''

        new_sql = '''
                    select t.* from (
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                    ) tab inner join info_court_tax_arrears t on t.court_id = tab.id;
                  '''

        self._sql_result_to_var(variable_name, "法院核查个人欠税名单", old_sql, new_sql)

    def _sql_result_to_var(self, variable_name, type_name, before_sql, after_sql):
        before_df = sql_to_df(sql=before_sql, params={
            "user_name": self.user_name,
            "id_card_no": self.id_card_no,
            "pre_biz_date": self.pre_biz_date
        })

        after_df = sql_to_df(sql=after_sql, params={
            "user_name": self.user_name,
            "id_card_no": self.id_card_no,
            "pre_biz_date": self.pre_biz_date
        })

        self.variables[variable_name] = {"type:": type_name, "before": json.loads(before_df.to_json(orient="records")),
                                         "after": json.loads(after_df.to_json(orient="records"))}

    def transform(self):
        # 前一个业务的创建时间
        self.pre_biz_date = self.origin_data.get('preBizDate')

        self._info_court_owed_owe("info_court_owed_owe")
        self._info_court_dishonesty("info_court_dishonesty")
        self._info_court_limit_entry("info_court_limit_entry")
        self._info_court_cri_sus("info_court_cri_sus")

        self._court_fin_loan_stats("info_court_fin_loan_con", "金融借款合同纠纷", "金融借款合同纠纷")
        self._court_fin_loan_stats("info_court_loan_con", "借款合同纠纷", "借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷")
        self._court_fin_loan_stats("info_court_pop_loan", "民间借贷纠纷", "民间借贷纠纷")

        self._info_court_admi_vio("info_court_admi_vio")
        self._info_court_judge("info_court_judge")
        self._info_court_trial_proc("info_court_trial_proc")
        self._info_court_tax_pay("info_court_tax_pay")
        self._info_court_pub_info("info_court_pub_info")
        self._info_court_tax_arrears("info_court_tax_arrears")

