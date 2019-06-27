import re

from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer, subtract_datetime_col,extract_money_court_administrative_violation,extract_money_court_excute_public


class T16002(Transformer):
    """
    法院核查 企业
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'court_ent_admi_vio': 0,
            'court_ent_judge': 0,
            'court_ent_trial_proc': 0,
            'court_ent_tax_pay': 0,
            'court_ent_owed_owe': 0,
            'court_ent_tax_arrears': 0,
            'court_ent_dishonesty': 0,
            'court_ent_limit_entry': 0,
            'court_ent_high_cons': 0,
            'court_ent_pub_info': 0,
            'court_ent_cri_sus': 0,
            'court_ent_tax_arrears_amt_3y': 0,
            'court_ent_pub_info_amt_3y': 0,
            'court_ent_admi_vio_amt_3y': 0,
            'court_ent_judge_amt_3y': 0,
            'court_ent_docu_status': 0,
            'court_ent_proc_status': 0
        }

    # 行政违法记录sql
    def _court_administrative_violation_df(self):
        info_court_administrative_violation = """
        SELECT A.create_time as create_time,B.execution_result as
        execution_result,B.specific_date as specific_date,B.court_id as court_id
        FROM info_court_administrative_violation B,(SELECT create_time,id FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_administrative_violation,
                       params={"user_name": self.user_name})
        self.dff_year = subtract_datetime_col(df, 'create_time', 'specific_date', 'Y')
        return df

    # 行政违法记录-数据处理
    def _ps_court_administrative_violation(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_admi_vio'] = df.shape[0]
            df = df.query(self.dff_year + ' < 3')
            df['max_money'] = df.apply(lambda x: extract_money_court_administrative_violation(x['execution_result']), axis=1)
            self.variables['court_ent_admi_vio_amt_3y'] = df['max_money'].sum()

    # 民商事裁判文书sql
    def _court_judicative_pape_df(self):
        info_court_judicative_pape = """
        SELECT A.create_time as create_time,B.legal_status as
        legal_status,B.case_amount as case_amount,B.closed_time as closed_time,B.court_id as court_id
        FROM info_court_judicative_pape B,(SELECT create_time,id FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_judicative_pape,
                       params={"user_name": self.user_name})
        self.dff_year = subtract_datetime_col(df, 'create_time', 'closed_time', 'Y')
        return df

    # 民商事裁判文书-数据处理
    def _ps_court_judicative_pape(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_judge'] = df.shape[0]

            if df[df['legal_status'].isnull() == False].shape[0] == 0:
                self.variables['court_ent_docu_status'] = 0
            elif df[df['legal_status'].str.contains('原告')].shape[0] == 0 and \
                    df[df['legal_status'].str.contains('被告')].shape[0] == 0:
                self.variables['court_ent_docu_status'] = 3
            elif df[df['legal_status'].str.contains('原告')].shape[0] > 0 and \
                    df[df['legal_status'].str.contains('被告')].shape[0] == 0:
                self.variables['court_ent_docu_status'] = 1
            elif df[df['legal_status'].str.contains('被告')].shape[0] > 0:
                self.variables['court_ent_docu_status'] = 2

            df = df.query(self.dff_year + ' < 3')
            self.variables['court_ent_judge_amt_3y'] = float('%.2f' % df['case_amount'].sum())

    # 民商事审判流程sql
    def _court_trial_process_df(self):
        info_court_trial_process = """
        SELECT A.create_time as create_time,B.specific_date as
        specific_date,B.legal_status as legal_status
        FROM info_court_trial_process B,(SELECT create_time,id FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_trial_process,
                       params={"user_name": self.user_name})
        return df

    # 民商事审判流程-数据处理
    def _ps_court_trial_process(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_trial_proc'] = df.shape[0]

            if df[df['legal_status'].isnull() == False].shape[0] == 0:
                self.variables['court_ent_proc_status'] = 0
            elif df[df['legal_status'].str.contains('原告')].shape[0] == 0 and \
                    df[df['legal_status'].str.contains('被告')].shape[0] == 0:
                self.variables['court_ent_proc_status'] = 3
            elif df[df['legal_status'].str.contains('原告')].shape[0] > 0 and \
                    df[df['legal_status'].str.contains('被告')].shape[0] == 0:
                self.variables['court_ent_proc_status'] = 1
            elif df[df['legal_status'].str.contains('被告')].shape[0] > 0:
                self.variables['court_ent_proc_status'] = 2

    # 纳税非正常户sql
    def _court_taxable_abnormal_user_df(self):
        info_court_taxable_abnormal_user = """
        SELECT A.create_time as create_time,B.confirm_date as
        confirm_date,B.court_id as court_id
        FROM info_court_taxable_abnormal_user B,(SELECT create_time,id FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_taxable_abnormal_user,
                       params={"user_name": self.user_name})
        return df

    # 纳税非正常户-数据处理
    def _ps_court_taxable_abnormal_user(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_tax_pay'] = df.shape[0]

    # 欠款欠费名单sql
    def _court_arrearage_df(self):
        info_court_arrearage = """
        SELECT A.create_time as create_time,B.default_amount as
        default_amount,B.default_date as default_date,B.court_id as court_id
        FROM info_court_arrearage B,(SELECT create_time,id FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_arrearage,
                       params={"user_name": self.user_name})
        return df

    # 欠款欠费名单-数据处理
    def _ps_court_arrearage(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_owed_owe'] = df.shape[0]

    # 欠税名单sql
    def _court_tax_arrears_df(self):
        info_court_tax_arrears = """
        SELECT A.create_time as create_time,B.taxes as
        taxes,B.taxes_time as taxes_time
        FROM info_court_tax_arrears B,(SELECT create_time FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_tax_arrears,
                       params={"user_name": self.user_name})
        self.dff_year = subtract_datetime_col(df, 'create_time', 'taxes_time', 'Y')
        return df

    # 欠税名单-数据处理
    def _ps_court_tax_arrears(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_tax_arrears'] = df.shape[0]
            df = df.query(self.dff_year + "< 3")
            self.variables['court_ent_tax_arrears_amt_3y'] = df['taxes'].sum()

    # 失信老赖名单sql
    def _court_deadbeat_df(self):
        info_court_deadbeat = """
        SELECT A.create_time as create_time,B.execute_content as
        execute_content,B.execute_date as execute_date,B.court_id as court_id
        FROM info_court_deadbeat B,(SELECT create_time,id FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_deadbeat,
                       params={"user_name": self.user_name})
        return df

    # 失信老赖名单-数据处理
    def _ps_court_deadbeat(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_dishonesty'] = df.shape[0]

    # 限制出入境sql
    def _court_limited_entry_exit_df(self):
        info_court_limited_entry_exit = """
        SELECT A.create_time as create_time,B.execute_content as
        execute_content,B.specific_date as specific_date,B.court_id as court_id
        FROM info_court_limited_entry_exit B,(SELECT create_time,id FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_limited_entry_exit,
                       params={"user_name": self.user_name})
        return df

    # 限制出入境-数据处理
    def _ps_court_limited_entry_exit(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_limit_entry'] = df.shape[0]

    # 限制高消费sql
    def _court_limit_hignspending_df(self):
        info_court_limit_hignspending = """
        SELECT A.create_time as create_time,B.execute_content as
        execute_content,B.specific_date as specific_date,B.court_id as court_id
        FROM info_court_limit_hignspending B,(SELECT create_time,id FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_limit_hignspending,
                       params={"user_name": self.user_name})
        return df

    # 限制高消费-数据处理
    def _ps_court_limit_hignspending(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_high_cons'] = df.shape[0]

    # 执行公开信息sql
    def _court_excute_public_df(self):
        info_court_excute_public = """
        SELECT A.create_time as create_time,B.execute_content as
        execute_content,B.filing_time as filing_time,B.court_id as court_id
        FROM info_court_excute_public B,(SELECT create_time,id FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_excute_public,
                       params={"user_name": self.user_name})
        self.dff_year = subtract_datetime_col(df, 'create_time', 'filing_time', 'Y')
        return df

    # 执行公开信息-数据处理
    def _ps_court_excute_public(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_pub_info'] = df.shape[0]
            df = df.query(self.dff_year + ' < 3')
            df['max_money'] = df.apply(lambda x: extract_money_court_excute_public(x['execute_content']), axis=1)
            self.variables['court_ent_pub_info_amt_3y'] = float("%.2f" % df['max_money'].sum())

    # 罪犯及嫌疑人名单sql
    def _court_criminal_suspect_df(self):
        info_court_criminal_suspect = """
        SELECT A.create_time as create_time,B.trial_date as
        trial_date,B.court_id as court_id
        FROM info_court_criminal_suspect B,(SELECT create_time,id FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_criminal_suspect,
                       params={"user_name": self.user_name})
        return df

    # 罪犯及嫌疑人名单-数据处理
    def _ps_court_criminal_suspect(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_cri_sus'] = df.shape[0]

    def transform(self, user_name=None, id_card_no=None, phone=None):
        self._ps_court_administrative_violation(df=self._court_administrative_violation_df())
        self._ps_court_judicative_pape(df=self._court_judicative_pape_df())
        self._ps_court_trial_process(df=self._court_trial_process_df())
        self._ps_court_taxable_abnormal_user(df=self._court_taxable_abnormal_user_df())
        self._ps_court_arrearage(df=self._court_arrearage_df())
        self._ps_court_deadbeat(df=self._court_deadbeat_df())
        self._ps_court_limited_entry_exit(df=self._court_limited_entry_exit_df())
        self._ps_court_limit_hignspending(df=self._court_limit_hignspending_df())
        self._ps_court_excute_public(df=self._court_excute_public_df())
        self._ps_court_criminal_suspect(df=self._court_criminal_suspect_df())
