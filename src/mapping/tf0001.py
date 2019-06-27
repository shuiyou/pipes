from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer,extract_money_court_administrative_violation,extract_money_court_excute_public
import pandas as pd

def check_is_contain(key,value):
    if key.find(value) >= 0:
        return 1
    else:
        return 0

class Tf0001(Transformer):
    """
    联企法院
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'relent_court_open_admi_violation': 0,
            'relent_court_open_judge_docu': 0,
            'relent_court_open_judge_proc': 0,
            'relent_court_open_tax_pay': 0,
            'relent_court_open_owed_owe': 0,
            'relent_court_open_tax_arrears': 0,
            'relent_court_open_court_dishonesty': 0,
            'relent_court_open_rest_entry': 0,
            'relent_court_open_high_cons': 0,
            'relent_court_open_pub_info': 0,
            'relent_court_open_cri_sus': 0,
            'relent_court_open_fin_loan_con': 0,
            'relent_court_open_loan_con': 0,
            'relent_court_open_pop_loan': 0,
            'relent_court_open_docu_status': 0,
            'relent_court_open_proc_status': 0,
            'relent_court_tax_arrears_max':0,
            'relent_court_pub_info_max':0,
            'relent_court_admi_violation_max':0,
            'relent_court_judge_max':0
        }

    #个人工商-法人信息
    def _per_bus_legal_df(self,status):
        info_per_bus_legal = """
       SELECT ent_name FROM info_per_bus_legal a,(SELECT id FROM info_per_bus_basic WHERE id_card_no = %(id_card_no)s and 
        unix_timestamp(NOW()) < unix_timestamp(expired_at)  ORDER BY expired_at desc LIMIT 1) b
        WHERE a.basic_id = b.id
        and a.ent_status in %(status)s;
        """
        df = sql_to_df(sql=info_per_bus_legal,
                       params={"id_card_no": self.id_card_no,
                               "status": status})
        return df

    #个人工商-股东信息
    def _per_bus_shareholder_df(self,status, ratio=0.2):
        info_per_bus_shareholder = """
        SELECT ent_name FROM info_per_bus_shareholder a,(SELECT id FROM info_per_bus_basic WHERE id_card_no = %(id_card_no)s and 
        unix_timestamp(NOW()) < unix_timestamp(expired_at)  ORDER BY expired_at desc LIMIT 1) b
        WHERE a.basic_id = b.id
        and a.ent_status in %(status)s
        and a.sub_conam/a.reg_cap >= %(ratio)s;
        """
        df = sql_to_df(sql=info_per_bus_shareholder,
                       params={"id_card_no": self.id_card_no,
                               "status": status,
                               "ratio":ratio})
        return df

    # 行政违法记录
    def _court_administrative_violation_df(self,df=None):
        info_court_administrative_violation = """
        SELECT execution_result
        FROM info_court_administrative_violation B,
        (SELECT id FROM info_court WHERE unique_name in %(unique_names)s AND expired_at > NOW()) A
        WHERE B.court_id = A.id
        """
        violation_df = sql_to_df(sql=info_court_administrative_violation,
                       params={"unique_names": df['ent_name'].unique().tolist()})
        return violation_df

    # 行政违法记录-数据处理
    def _ps_court_administrative_violation(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_admi_violation'] = df.shape[0]
            df['max_money'] = df.apply(lambda x: extract_money_court_administrative_violation(x['execution_result']),axis=1)
            self.variables['relent_court_admi_violation_max'] = df['max_money'].max()

    #民商事裁判文书
    def _court_judicative_pape_df(self, df=None):
        info_court_judicative_pape = """
        SELECT case_reason,legal_status,case_amount
        FROM info_court_judicative_pape B,
        (SELECT id FROM info_court WHERE unique_name in %(unique_names)s AND expired_at > NOW()) A
        WHERE B.court_id = A.id
        """
        judicative_df = sql_to_df(sql=info_court_judicative_pape,
                                 params={"unique_names": df['ent_name'].unique().tolist()})
        return judicative_df

    # 民商事裁判文书-数据处理
    def _ps_court_judicative_pape(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_judge_docu'] = df.shape[0]
            self.variables['relent_court_judge_max'] = df['case_amount'].max()

    # 民商事审判流程sql
    def _court_trial_process_df(self, df=None):
        info_court_trial_process = """
        SELECT case_reason,legal_status
        FROM info_court_trial_process B,(SELECT id FROM info_court WHERE unique_name in %(unique_names)s
        AND expired_at > NOW() ) A
        WHERE B.court_id = A.id
        """
        trial_df = sql_to_df(sql=info_court_trial_process,
                             params={"unique_names": df['ent_name'].unique().tolist()})
        return trial_df

    # 民商事审判流程-数据处理
    def _ps_court_trial_process(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_judge_proc'] = df.shape[0]

    # 纳税非正常户
    def _court_taxable_abnormal_user_df(self, df=None):
        info_court_taxable_abnormal_user = """
        SELECT confirm_date
        FROM info_court_taxable_abnormal_user B,(SELECT id FROM info_court WHERE unique_name in %(unique_names)s
        AND expired_at > NOW() ) A
        WHERE B.court_id = A.id
        """
        taxable_df = sql_to_df(sql=info_court_taxable_abnormal_user,
                               params={"unique_names": df['ent_name'].unique().tolist()})
        return taxable_df

    # 纳税非正常户-数据处理
    def _ps_court_taxable_abnormal_user(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_tax_pay'] = df.shape[0]

    # 欠款欠费名单sql
    def _court_arrearage_df(self, df=None):
        info_court_arrearage = """
        SELECT default_amount
        FROM info_court_arrearage B,(SELECT id FROM info_court WHERE unique_name in %(unique_names)s
        AND expired_at > NOW() ) A
        WHERE B.court_id = A.id
        """
        arrearage_df = sql_to_df(sql=info_court_arrearage,
                       params={"unique_names": df['ent_name'].unique().tolist()})
        return arrearage_df

     # 欠款欠费名单-数据处理
    def _ps_court_arrearage(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_owed_owe'] = df.shape[0]

    # 欠税名单sql
    def _court_tax_arrears_df(self, df=None):
        info_court_tax_arrears = """
        SELECT taxes
        FROM info_court_tax_arrears B,(SELECT id FROM info_court WHERE unique_name in %(unique_names)s
        AND expired_at > NOW() ) A
        WHERE B.court_id = A.id
        """
        tax_df = sql_to_df(sql=info_court_tax_arrears,
                       params={"unique_names": df['ent_name'].unique().tolist()})
        return tax_df

    # 欠税名单-数据处理
    def _ps_court_tax_arrears(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_tax_arrears'] = df.shape[0]
            self.variables['relent_court_tax_arrears_max'] = df['taxes'].max()

    # 失信老赖名单sql
    def _court_deadbeat_df(self, df=None):
        info_court_deadbeat = """
        SELECT execute_content
        FROM info_court_deadbeat B,(SELECT id FROM info_court WHERE unique_name in %(unique_names)s
        AND expired_at > NOW() ) A
        WHERE B.court_id = A.id
        """
        deadbeat_df = sql_to_df(sql=info_court_deadbeat,
                       params={"unique_names": df['ent_name'].unique().tolist()})
        return deadbeat_df

    # 失信老赖名单-数据处理
    def _ps_court_deadbeat(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_court_dishonesty'] = df.shape[0]

    # 限制出入境sql
    def _court_limited_entry_exit_df(self, df=None):
        info_court_limited_entry_exit = """
        SELECT execute_content
        FROM info_court_limited_entry_exit B,(SELECT id FROM info_court WHERE unique_name in %(unique_names)s
        AND expired_at > NOW() ) A
        WHERE B.court_id = A.id
        """
        exit_df = sql_to_df(sql=info_court_limited_entry_exit,
                       params={"unique_names": df['ent_name'].unique().tolist()})
        return exit_df

    # 限制出入境-数据处理
    def _ps_court_limited_entry_exit(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_rest_entry'] = df.shape[0]

    # 限制高消费sql
    def _court_limit_hignspending_df(self, df=None):
        info_court_limit_hignspending = """
        SELECT execute_content
        FROM info_court_limit_hignspending B,(SELECT id FROM info_court WHERE unique_name in %(unique_names)s
        AND expired_at > NOW() ) A
        WHERE B.court_id = A.id
        """
        hignspending_df = sql_to_df(sql=info_court_limit_hignspending,
                       params={"unique_names": df['ent_name'].unique().tolist()})
        return hignspending_df

    # 限制高消费-数据处理
    def _ps_court_limit_hignspending(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_high_cons'] = df.shape[0]

    # 罪犯及嫌疑人名单sql
    def _court_criminal_suspect_df(self, df=None):
        info_court_criminal_suspect = """
        SELECT trial_date
        FROM info_court_criminal_suspect B,(SELECT id FROM info_court WHERE unique_name in %(unique_names)s
        AND expired_at > NOW() ) A
        WHERE B.court_id = A.id
        """
        suspect_df = sql_to_df(sql=info_court_criminal_suspect,
                       params={"unique_names": df['ent_name'].unique().tolist()})
        return suspect_df

    # 罪犯及嫌疑人名单-数据处理
    def _ps_court_criminal_suspect(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_cri_sus'] = df.shape[0]

    #各种类型的纠纷案件
    def _ps_dispute(self,df=None):
        if df is not None and len(df) > 0:
            df['legal_status_contain'] = df.apply(lambda x:check_is_contain(x['legal_status'],"被告"),axis=1)
            if df.query('legal_status_contain > 0 and "金融借款合同纠纷" in case_reason').shape[0] > 0:
                self.variables['relent_court_open_fin_loan_con'] = 1
            if df.query('legal_status_contain > 0 and "借款合同纠纷" in case_reason').shape[0] > 0:
                self.variables['relent_court_open_loan_con'] = 1
            if df.query('legal_status_contain > 0 and "民间借贷纠纷" in case_reason').shape[0] > 0:
                self.variables['relent_court_open_pop_loan'] = 1

    #裁判文书/审判流程诉讼地位标识
    def _ps_judicative_litigation(self,df=None):
        if df is not None and len(df) > 0:
            df = df.dropna(subset=['legal_status'],how='any')
            df['legal_status_defendant'] = df.apply(lambda x:check_is_contain(x['legal_status'],"被告"),axis=1)
            df['legal_status_plaintiff'] = df.apply(lambda x:check_is_contain(x['legal_status'],"原告"),axis=1)
            number_defendant = df.query('legal_status_defendant > 0').shape[0]
            number_plaintiff = df.query('legal_status_plaintiff > 0').shape[0]
            number_total = df.shape[0]
            if number_plaintiff > 0 and number_plaintiff == number_total:
                return 1
            elif number_defendant > 0:
                return 2
            elif number_defendant ==0 and number_plaintiff < number_total:
                return 3

    # 执行公开信息
    def _court_excute_public_df(self, df=None):
        info_court_excute_public = """
        SELECT execute_content
        FROM info_court_excute_public B,(SELECT id FROM info_court WHERE unique_name in %(unique_names)s
        AND expired_at > NOW() ) A
        WHERE B.court_id = A.id
        """
        public_df = sql_to_df(sql=info_court_excute_public,
                       params={"unique_names": df['ent_name'].unique().tolist()})
        return public_df

    # 执行公开信息-数据处理
    def _ps_court_excute_public(self, df=None):
        if df is not None and len(df) > 0:
            df['max_money'] = df.apply(lambda x: extract_money_court_excute_public(x['execute_content']), axis=1)
            self.variables['relent_court_pub_info_max'] = df['max_money'].max()


    def transform(self):
        ent_on_status = ['在营（开业）', '存续（在营、开业、在册）']
        legal_df = self._per_bus_legal_df(status=ent_on_status)
        shareholder_df = self._per_bus_shareholder_df(status=ent_on_status)
        concat_df = pd.concat([shareholder_df, legal_df])
        # 行政违法记录
        violation_df =  self._court_administrative_violation_df(df=concat_df)
        self._ps_court_administrative_violation(df = violation_df)
        #民商事裁判文书
        judicative_df = self._court_judicative_pape_df(df=concat_df)
        self._ps_court_judicative_pape(df = judicative_df)
        #民商事审判流程
        trial_df = self._court_trial_process_df(df=concat_df)
        self._ps_court_trial_process(df = trial_df)
        #纳税非正常户
        taxable_df = self._court_taxable_abnormal_user_df(df=concat_df)
        self._ps_court_taxable_abnormal_user(df=taxable_df)
        #欠款欠费名单
        arrearage_df = self._court_arrearage_df(df=concat_df)
        self._ps_court_arrearage(df = arrearage_df)
        #欠税名单
        tax_df = self._court_tax_arrears_df(df=concat_df)
        self._ps_court_tax_arrears(df = tax_df)
        #失信老赖名单
        deadbeat_df = self._court_deadbeat_df(df=concat_df)
        self._ps_court_deadbeat(df = deadbeat_df)
        #限制出入境名单
        exit_df = self._court_limited_entry_exit_df(df=concat_df)
        self._ps_court_limited_entry_exit(df = exit_df)
        #限制高消费名单
        hignspending_df = self._court_limit_hignspending_df(df=concat_df)
        self._ps_court_limit_hignspending(df = hignspending_df)
        #罪犯及嫌疑人名单
        suspect_df = self._court_criminal_suspect_df(df=concat_df)
        self._ps_court_criminal_suspect(df = suspect_df)
        #是否存在各种类型的纠纷案件
        dispute_df = pd.concat([judicative_df, trial_df])
        self._ps_dispute(df=dispute_df)
        #裁判文书诉讼地位标识
        self.variables['relent_court_open_docu_status'] = self._ps_judicative_litigation(judicative_df)
        #审判流程诉讼地位标识
        self.variables['relent_court_open_proc_status'] = self._ps_judicative_litigation(trial_df)
        #执行公开信息最大金额
        public_df = self._court_excute_public_df(df=concat_df)
        self._ps_court_excute_public(df = public_df)




