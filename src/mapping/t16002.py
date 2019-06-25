from mapping.tranformer import Transformer, subtract_datetime_col
from mapping.mysql_reader import sql_to_df
import re


def _get_maxMoney_from_string(value):
    value = re.sub(r"(第?[0-9]\d*号)", "", value)
    value = re.sub(r"\([0-9]{4}\)", "", value)
    value = re.sub(r"〔[0-9]{4}〕", "", value)
    value = re.sub(r"(http://)((\w)|(\W)|(\.))*", "", value)
    moneyArray = re.findall(r"\d+\.?\d*", value)
    moneyMax = 0
    for money in moneyArray:
        if ('万元') in value:
            moneyRe = float(money) * 10000
        else:
            moneyRe = float(money)
        if moneyRe > moneyMax:
            moneyMax = moneyRe
    return "%.2f" % moneyMax


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

    #行政违法记录sql
    def _court_administrative_violation_df(self,user_name):
        info_court_administrative_violation = """
        SELECT A.create_time as create_time,B.execution_result as
        execution_result,B.specific_date as specific_date
        FROM info_court_administrative_violation B,(SELECT create_time FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_administrative_violation,
                       params={"user_name": user_name})
        self.dff_year = subtract_datetime_col(df, 'create_time', 'specific_date', 'Y')
        return df

    #行政违法记录-数据处理
    def _ps_court_administrative_violation(self,df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_admi_vio'] = df.shape[0]
            df = df.query(self.dff_year + ' < 3')
            df['max_money'] = df.apply(lambda x: _get_maxMoney_from_string(df['execution_result']), axis=1)
            self.variables['court_ent_admi_vio_amt_3y'] = df['max_money'].sum()

    #民商事裁判文书sql
    def _court_judicative_pape_df(self,user_name):
        info_court_judicative_pape = """
        SELECT A.create_time as create_time,B.legal_status as
        legal_status,B.case_amount as case_amount,B.closed_time as closed_time
        FROM info_court_judicative_pape B,(SELECT create_time FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_judicative_pape,
                       params={"user_name": user_name})
        self.dff_year = subtract_datetime_col(df, 'create_time', 'closed_time', 'Y')
        return df

    #民商事裁判文书-数据处理
    def _ps_court_judicative_pape(self,df=None):
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
            self.variables['court_ent_judge_amt_3y'] = df['case_amount'].sum()

    #民商事审判流程sql
    def _court_trial_process_df(self,user_name):
        info_court_trial_process = """
        SELECT A.create_time as create_time,B.specific_date as
        specific_date,B.legal_status as legal_status
        FROM info_court_trial_process B,(SELECT create_time FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_trial_process,
                       params={"user_name": user_name})
        return df

    # 民商事审判流程-数据处理
    def _ps_court_trial_process(self,df=None):
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

    #纳税非正常户sql
    def _court_taxable_abnormal_user_df(self,user_name):
        info_court_taxable_abnormal_user = """
        SELECT A.create_time as create_time,B.confirm_date as
        confirm_date
        FROM info_court_taxable_abnormal_user B,(SELECT create_time FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_taxable_abnormal_user,
                       params={"user_name": user_name})
        return df

    #纳税非正常户-数据处理
    def _ps_court_taxable_abnormal_user(self,df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_tax_pay'] = df.shape[0]

    #欠款欠费名单sql
    def _court_arrearage_df(self,user_name):
        info_court_arrearage = """
        SELECT A.create_time as create_time,B.default_amount as
        default_amount,B.default_date as default_date
        FROM info_court_arrearage B,(SELECT create_time FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_arrearage,
                       params={"user_name": user_name})
        return df

    #欠款欠费名单-数据处理
    def _ps_court_arrearage(self,df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_owed_owe'] = df.shape[0]

    #欠税名单sql
    def _court_tax_arrears_df(self,user_name):
        info_court_tax_arrears = """
        SELECT A.create_time as create_time,B.taxes as
        taxes,B.taxes_time as taxes_time
        FROM info_court_tax_arrears B,(SELECT create_time FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_tax_arrears,
                       params={"user_name": user_name})
        self.dff_year = subtract_datetime_col(df, 'create_time', 'taxes_time', 'Y')
        return df

    # 欠税名单-数据处理
    def _ps_court_tax_arrears(self,df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_tax_arrears'] = df.shape[0]
            df = df.query(self.dff_year + "< 3")
            self.variables['court_ent_tax_arrears_amt_3y'] = df['taxes'].sum()

    #失信老赖名单sql
    def _court_deadbeat_df(self,user_name):
        info_court_deadbeat = """
        SELECT A.create_time as create_time,B.execute_content as
        execute_content,B.execute_date as execute_date
        FROM info_court_deadbeat B,(SELECT create_time FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_deadbeat,
                       params={"user_name": user_name})
        return df

    # 失信老赖名单-数据处理
    def _ps_court_deadbeat(self,df=None):
       if df is not None and len(df) > 0:
            self.variables['court_ent_dishonesty'] = df.shape[0]

    #限制出入境sql
    def _court_limited_entry_exit_df(self,user_name):
        info_court_limited_entry_exit = """
        SELECT A.create_time as create_time,B.execute_content as
        execute_content,B.specific_date as specific_date
        FROM info_court_limited_entry_exit B,(SELECT create_time FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_limited_entry_exit,
                       params={"user_name": user_name})
        return df

    # 限制出入境-数据处理
    def _ps_court_limited_entry_exit(self,df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_limit_entry'] = df.shape[0]

    #限制高消费sql
    def _court_limit_hignspending_df(self,user_name):
        info_court_limit_hignspending = """
        SELECT A.create_time as create_time,B.execute_content as
        execute_content,B.specific_date as specific_date
        FROM info_court_limit_hignspending B,(SELECT create_time FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_limit_hignspending,
                       params={"user_name": user_name})
        return df

    #限制高消费-数据处理
    def _ps_court_limit_hignspending(self,df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_high_cons'] = df.shape[0]

    #执行公开信息sql
    def _court_excute_public_df(self,user_name):
        info_court_excute_public = """
        SELECT A.create_time as create_time,B.execute_content as
        execute_content,B.filing_time as filing_time
        FROM info_court_excute_public B,(SELECT create_time FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_excute_public,
                       params={"user_name": user_name})
        self.dff_year = subtract_datetime_col(df, 'create_time', 'filing_time', 'Y')
        return df

    # 执行公开信息-数据处理
    def _ps_court_excute_public(self,df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_pub_info'] = df.shape[0]
            df = df.query(self.dff_year + ' < 3')
            df['max_money'] = df.apply(lambda x:_get_maxMoney_from_string(df['execute_content']),axis=1)
            self.variables['court_ent_pub_info_amt_3y'] = df['max_money'].sum()

    #罪犯及嫌疑人名单sql
    def _court_criminal_suspect_df(self,user_name):
        info_court_criminal_suspect = """
        SELECT A.create_time as create_time,B.trial_date as
        trial_date
        FROM info_court_criminal_suspect B,(SELECT create_time FROM info_court WHERE unique_name = %(user_name)s
        AND expired_at > NOW() ORDER BY expired_at DESC LIMIT 1) A
        WHERE B.court_id = A.id
        """
        df = sql_to_df(sql=info_court_criminal_suspect,
                       params={"user_name": user_name})
        return df

    #罪犯及嫌疑人名单-数据处理
    def _ps_court_criminal_suspect(self,df=None):
        if df is not None and len(df) > 0:
            self.variables['court_ent_cri_sus'] = df.shape[0]


    def transform(self, user_name=None, id_card_no=None, phone=None):
        self._ps_court_administrative_violation(self._court_administrative_violation_df(self.user_name))
        self._ps_court_judicative_pape(self._court_judicative_pape_df(self.user_name))
        self._ps_court_trial_process(self._court_trial_process_df(self.user_name))
        self._ps_court_taxable_abnormal_user(self._court_tax_arrears_df(self.user_name))
        self._ps_court_arrearage(self._court_arrearage_df(self.user_name))
        self._ps_court_deadbeat(self._court_deadbeat_df(self.user_name))
        self._ps_court_limited_entry_exit(self._court_limited_entry_exit_df(self.user_name))
        self._ps_court_limit_hignspending(self._court_limit_hignspending_df(self.user_name))
        self._ps_court_excute_public(self._court_excute_public_df(self.user_name))
        self._ps_court_criminal_suspect(self._court_criminal_suspect_df(self.user_name))
