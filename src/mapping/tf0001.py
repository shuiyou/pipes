import pandas as pd

from util.mysql_reader import sql_to_df
from mapping.tranformer import Transformer, extract_money, \
    extract_money_court_excute_public


def check_is_contain(key, value):
    if key.find(value) >= 0:
        return 1
    else:
        return 0

def get_out_decision_code(df,court_df):
    # df-court_id去重
    df = df.drop_duplicates(subset=['id'])
    # 与court_df merge left,取出主体
    court_merge_df = pd.merge(df, court_df, on=['id'], how='left')
    # 遍历新的df，拼接out_decision_code
    array = []
    for index, row in court_merge_df.iterrows():
        value = {
            'name': row['unique_name'],
            'idno': row['unique_id_no']
        }
        array.append(value)
    return array


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
            'relent_court_tax_arrears_max': 0,
            'relent_court_pub_info_max': 0,
            'relent_court_admi_violation_max': 0,
            'relent_court_judge_max': 0
        }

    # 个人工商-法人信息
    def _per_bus_legal_df(self, status):
        info_per_bus_legal = """
       SELECT ent_name FROM info_per_bus_legal a,(SELECT id FROM info_per_bus_basic WHERE id_card_no = %(id_card_no)s and 
        unix_timestamp(NOW()) < unix_timestamp(expired_at)  ORDER BY id  desc LIMIT 1) b
        WHERE a.basic_id = b.id
        and a.ent_status in %(status)s;
        """
        df = sql_to_df(sql=info_per_bus_legal,
                       params={"id_card_no": self.id_card_no,
                               "status": status})
        return df

    # 个人工商-股东信息
    def _per_bus_shareholder_df(self, status, ratio=0.2):
        info_per_bus_shareholder = """
        SELECT ent_name FROM info_per_bus_shareholder a,(SELECT id FROM info_per_bus_basic WHERE id_card_no = %(id_card_no)s and 
        unix_timestamp(NOW()) < unix_timestamp(expired_at)  ORDER BY id  desc LIMIT 1) b
        WHERE a.basic_id = b.id
        and a.ent_status in %(status)s
        and a.sub_conam/a.reg_cap >= %(ratio)s;
        """
        df = sql_to_df(sql=info_per_bus_shareholder,
                       params={"id_card_no": self.id_card_no,
                               "status": status,
                               "ratio": ratio})
        return df

    def _court_info_df(self, df=None):
        info_court = """
        SELECT id,unique_name,unique_id_no FROM info_court WHERE unique_name in %(unique_names)s
        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
        """
        court_df = sql_to_df(sql=info_court,
                             params={"unique_names": df['ent_name'].unique().tolist()})
        if court_df is not None and len(court_df) > 0:
            court_group_df = court_df[['id', 'unique_name']].groupby(by='unique_name', as_index=False).max()
            court_merge_df = pd.merge(court_group_df, court_df, on=['id', 'unique_name'], how='left')
            return court_merge_df

    # 行政违法记录
    def _court_administrative_violation_df(self, df=None):
        info_court_administrative_violation = """
        SELECT execution_result,court_id as id
        FROM info_court_administrative_violation
        WHERE court_id in %(ids)s
        """
        violation_df = sql_to_df(sql=info_court_administrative_violation,
                                 params={"ids": df['id'].unique().tolist()})

        return violation_df

    # 行政违法记录-数据处理
    def _ps_court_administrative_violation(self, df=None,court_df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_admi_violation'] = df.shape[0]
            if self.variables['relent_court_open_admi_violation']>0:
                administrative_violation_array = get_out_decision_code(df=df,court_df=court_df)
                self.out_decision_code['XT003'] = administrative_violation_array
            df['max_money'] = df.apply(lambda x: extract_money(x['execution_result']),
                                       axis=1)
            self.variables['relent_court_admi_violation_max'] = df['max_money'].max()

    # 民商事裁判文书
    def _court_judicative_pape_df(self, df=None):
        info_court_judicative_pape = """
        SELECT case_reason,legal_status,case_amount,court_id as id
        FROM info_court_judicative_pape 
        WHERE court_id in %(ids)s
        """
        judicative_df = sql_to_df(sql=info_court_judicative_pape,
                                  params={"ids": df['id'].unique().tolist()})
        return judicative_df

    # 民商事裁判文书-数据处理
    def _ps_court_judicative_pape(self, df=None,court_df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_judge_docu'] = df.shape[0]
            if self.variables['relent_court_open_judge_docu']>0:
                judicative_pape_array = get_out_decision_code(df=df,court_df=court_df)
                self.out_decision_code['XT004'] = judicative_pape_array
            self.variables['relent_court_judge_max'] = df['case_amount'].max()

    # 民商事审判流程sql
    def _court_trial_process_df(self, df=None):
        info_court_trial_process = """
        SELECT case_reason,legal_status,court_id as id
        FROM info_court_trial_process 
        WHERE court_id in %(ids)s
        """
        trial_df = sql_to_df(sql=info_court_trial_process,
                             params={"ids": df['id'].unique().tolist()})
        return trial_df

    # 民商事审判流程-数据处理
    def _ps_court_trial_process(self, df=None,court_df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_judge_proc'] = df.shape[0]
            if self.variables['relent_court_open_judge_proc']>0:
                trial_process_array = get_out_decision_code(df=df,court_df=court_df)
                self.out_decision_code['XT005'] = trial_process_array

    # 纳税非正常户
    def _court_taxable_abnormal_user_df(self, df=None):
        info_court_taxable_abnormal_user = """
        SELECT confirm_date,court_id as id
        FROM info_court_taxable_abnormal_user 
        WHERE court_id in %(ids)s
        """
        taxable_df = sql_to_df(sql=info_court_taxable_abnormal_user,
                               params={"ids": df['id'].unique().tolist()})
        return taxable_df

    # 纳税非正常户-数据处理
    def _ps_court_taxable_abnormal_user(self, df=None,court_df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_tax_pay'] = df.shape[0]
            if self.variables['relent_court_open_tax_pay']>0:
                abnormal_user_array = get_out_decision_code(df=df,court_df=court_df)
                self.out_decision_code['XT006'] = abnormal_user_array

    # 欠款欠费名单sql
    def _court_arrearage_df(self, df=None):
        info_court_arrearage = """
        SELECT default_amount,court_id as id
        FROM info_court_arrearage 
        WHERE court_id in %(ids)s
        """
        arrearage_df = sql_to_df(sql=info_court_arrearage,
                                 params={"ids": df['id'].unique().tolist()})
        return arrearage_df

    # 欠款欠费名单-数据处理
    def _ps_court_arrearage(self, df=None,court_df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_owed_owe'] = df.shape[0]
            if self.variables['relent_court_open_owed_owe'] > 0:
                array = get_out_decision_code(df=df,court_df=court_df)
                self.out_decision_code['X001'] = array
                self.out_decision_code['XM001'] = array

    # 欠税名单sql
    def _court_tax_arrears_df(self, df=None):
        info_court_tax_arrears = """
        SELECT taxes,court_id as id
        FROM info_court_tax_arrears 
        WHERE court_id in %(ids)s
        """
        tax_df = sql_to_df(sql=info_court_tax_arrears,
                           params={"ids": df['id'].unique().tolist()})
        return tax_df

    # 欠税名单-数据处理
    def _ps_court_tax_arrears(self, df=None,court_df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_tax_arrears'] = df.shape[0]
            if self.variables['relent_court_open_tax_arrears']>0:
                tax_arrears_array = get_out_decision_code(df=df,court_df=court_df)
                self.out_decision_code['XT001'] = tax_arrears_array
            self.variables['relent_court_tax_arrears_max'] = df['taxes'].max()

    # 失信老赖名单sql
    def _court_deadbeat_df(self, df=None):
        info_court_deadbeat = """
        SELECT execute_content,court_id as id
        FROM info_court_deadbeat 
        WHERE court_id in %(ids)s
        """
        deadbeat_df = sql_to_df(sql=info_court_deadbeat,
                                params={"ids": df['id'].unique().tolist()})
        return deadbeat_df

    # 失信老赖名单-数据处理
    def _ps_court_deadbeat(self, df=None,court_df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_court_dishonesty'] = df.shape[0]
            if self.variables['relent_court_open_court_dishonesty'] > 0:
                array = get_out_decision_code(df=df,court_df=court_df)
                self.out_decision_code['X002'] = array
                self.out_decision_code['XM002'] = array

            # 限制出入境sql
    def _court_limited_entry_exit_df(self, df=None):
        info_court_limited_entry_exit = """
        SELECT execute_content,court_id as id
        FROM info_court_limited_entry_exit 
        WHERE court_id in %(ids)s
        """
        exit_df = sql_to_df(sql=info_court_limited_entry_exit,
                            params={"ids": df['id'].unique().tolist()})
        return exit_df

    # 限制出入境-数据处理
    def _ps_court_limited_entry_exit(self, df=None,court_df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_rest_entry'] = df.shape[0]
            if self.variables['relent_court_open_rest_entry']>0:
                court_limited_array = get_out_decision_code(df=df,court_df=court_df)
                self.out_decision_code['X003'] = court_limited_array
                self.out_decision_code['XM003'] = court_limited_array

                # 限制高消费sql
    def _court_limit_hignspending_df(self, df=None):
        info_court_limit_hignspending = """
        SELECT execute_content,court_id as id
        FROM info_court_limit_hignspending 
        WHERE court_id in %(ids)s
        """
        hignspending_df = sql_to_df(sql=info_court_limit_hignspending,
                                    params={"ids": df['id'].unique().tolist()})
        return hignspending_df

    # 限制高消费-数据处理
    def _ps_court_limit_hignspending(self, df=None,court_df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_high_cons'] = df.shape[0]
            if self.variables['relent_court_open_high_cons']>0:
                limit_hignspending_array = get_out_decision_code(df=df,court_df=court_df)
                self.out_decision_code['X004'] = limit_hignspending_array
                self.out_decision_code['XM004'] = limit_hignspending_array

                # 罪犯及嫌疑人名单sql
    def _court_criminal_suspect_df(self, df=None):
        info_court_criminal_suspect = """
        SELECT trial_date,court_id as id
        FROM info_court_criminal_suspect 
        WHERE court_id in %(ids)s
        """
        suspect_df = sql_to_df(sql=info_court_criminal_suspect,
                               params={"ids": df['id'].unique().tolist()})
        return suspect_df

    # 罪犯及嫌疑人名单-数据处理
    def _ps_court_criminal_suspect(self, df=None,court_df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_cri_sus'] = df.shape[0]
            if self.variables['relent_court_open_cri_sus']>0:
                criminal_suspect_array = get_out_decision_code(df=df,court_df=court_df)
                self.out_decision_code['X005'] = criminal_suspect_array
                self.out_decision_code['XM005'] = criminal_suspect_array

                # 各种类型的纠纷案件
    def _ps_dispute(self, df=None,court_df=None):
        if df is not None and len(df) > 0:
            df = df.dropna(subset=['legal_status'], how='any')
            if df is not None and len(df) > 0:
                df['legal_status_contain'] = df.apply(lambda x: check_is_contain(x['legal_status'], "被告"), axis=1)
                lend_loan_df = df.query('legal_status_contain > 0 and "金融借款合同纠纷" in case_reason')
                if lend_loan_df.shape[0] > 0:
                    self.variables['relent_court_open_fin_loan_con'] = 1
                    lend_loan_array = get_out_decision_code(df=lend_loan_df,court_df=court_df)
                    self.out_decision_code['X006'] = lend_loan_array
                    self.out_decision_code['XM006'] = lend_loan_array
                loan_df = df.query('legal_status_contain > 0 and "借款合同纠纷" in case_reason')
                if loan_df.shape[0] > 0:
                    self.variables['relent_court_open_loan_con'] = 1
                    loan_array = get_out_decision_code(df=loan_df, court_df=court_df)
                    self.out_decision_code['X007'] = loan_array
                    self.out_decision_code['XM007'] = loan_array
                private_lend_df = df.query('legal_status_contain > 0 and "民间借贷纠纷" in case_reason')
                if private_lend_df.shape[0] > 0:
                    self.variables['relent_court_open_pop_loan'] = 1
                    private_lend_array = get_out_decision_code(df=private_lend_df, court_df=court_df)
                    self.out_decision_code['X008'] = private_lend_array
                    self.out_decision_code['XM008'] = private_lend_array

    # 裁判文书/审判流程诉讼地位标识
    def _ps_judicative_litigation(self, df=None):
        if df is not None and len(df) > 0:
            df = df.dropna(subset=['legal_status'], how='any')
            if df is not None and len(df) > 0:
                df['legal_status_defendant'] = df.apply(lambda x: check_is_contain(x['legal_status'], "被告"), axis=1)
                df['legal_status_plaintiff'] = df.apply(lambda x: check_is_contain(x['legal_status'], "原告"), axis=1)
                number_defendant = df.query('legal_status_defendant > 0').shape[0]
                number_plaintiff = df.query('legal_status_plaintiff > 0').shape[0]
                number_total = df.shape[0]
                if number_plaintiff > 0 and number_plaintiff == number_total:
                    return 1
                elif number_defendant > 0:
                    return 2
                elif number_defendant == 0 and number_plaintiff < number_total:
                    return 3
            else:
                return 0

    # 执行公开信息
    def _court_excute_public_df(self, df=None):
        info_court_excute_public = """
        SELECT execute_content,court_id as id
        FROM info_court_excute_public
        WHERE court_id in %(ids)s
        """
        public_df = sql_to_df(sql=info_court_excute_public,
                              params={"ids": df['id'].unique().tolist()})
        return public_df

    # 执行公开信息-数据处理
    def _ps_court_excute_public(self, df=None,court_df=None):
        if df is not None and len(df) > 0:
            self.variables['relent_court_open_pub_info'] = len(df)
            if self.variables['relent_court_open_pub_info']>0:
                excute_public_array = get_out_decision_code(df=df,court_df=court_df)
                self.out_decision_code['XT002'] = excute_public_array
            df['max_money'] = df.apply(lambda x: extract_money_court_excute_public(x['execute_content']), axis=1)
            self.variables['relent_court_pub_info_max'] = df['max_money'].max()

    def transform(self):
        ent_on_status = ['在营（开业）', '存续（在营、开业、在册）']
        legal_df = self._per_bus_legal_df(status=ent_on_status)
        shareholder_df = self._per_bus_shareholder_df(status=ent_on_status)
        concat_df = pd.concat([shareholder_df, legal_df])
        if concat_df is not None and concat_df['ent_name'].shape[0] > 0:
            # 查出法院核查主表的ids
            court_merge_df = self._court_info_df(df=concat_df)
            if court_merge_df is not None and len(court_merge_df) > 0:
                # 行政违法记录
                violation_df = self._court_administrative_violation_df(df=court_merge_df)
                self._ps_court_administrative_violation(df=violation_df,court_df=court_merge_df)
                # 民商事裁判文书
                judicative_df = self._court_judicative_pape_df(df=court_merge_df)
                self._ps_court_judicative_pape(df=judicative_df,court_df=court_merge_df)
                # 民商事审判流程
                trial_df = self._court_trial_process_df(df=court_merge_df)
                self._ps_court_trial_process(df=trial_df,court_df=court_merge_df)
                # 纳税非正常户
                taxable_df = self._court_taxable_abnormal_user_df(df=court_merge_df)
                self._ps_court_taxable_abnormal_user(df=taxable_df,court_df=court_merge_df)
                # 欠款欠费名单
                arrearage_df = self._court_arrearage_df(df=court_merge_df)
                self._ps_court_arrearage(df=arrearage_df,court_df=court_merge_df)
                # 欠税名单
                tax_df = self._court_tax_arrears_df(df=court_merge_df)
                self._ps_court_tax_arrears(df=tax_df,court_df=court_merge_df)
                # 失信老赖名单
                deadbeat_df = self._court_deadbeat_df(df=court_merge_df)
                self._ps_court_deadbeat(df=deadbeat_df,court_df=court_merge_df)
                # 限制出入境名单
                exit_df = self._court_limited_entry_exit_df(df=court_merge_df)
                self._ps_court_limited_entry_exit(df=exit_df,court_df=court_merge_df)
                # 限制高消费名单
                hignspending_df = self._court_limit_hignspending_df(df=court_merge_df)
                self._ps_court_limit_hignspending(df=hignspending_df,court_df=court_merge_df)
                # 罪犯及嫌疑人名单
                suspect_df = self._court_criminal_suspect_df(df=court_merge_df)
                self._ps_court_criminal_suspect(df=suspect_df,court_df=court_merge_df)
                # 是否存在各种类型的纠纷案件
                dispute_df = pd.concat([judicative_df, trial_df])
                self._ps_dispute(df=dispute_df,court_df=court_merge_df)
                # 裁判文书诉讼地位标识
                if judicative_df is not None and len(judicative_df) > 0:
                    self.variables['relent_court_open_docu_status'] = self._ps_judicative_litigation(judicative_df)
                # 审判流程诉讼地位标识
                if trial_df is not None and len(trial_df) > 0:
                    self.variables['relent_court_open_proc_status'] = self._ps_judicative_litigation(trial_df)
                # 执行公开信息最大金额
                public_df = self._court_excute_public_df(df=court_merge_df)
                self._ps_court_excute_public(df=public_df,court_df=court_merge_df)
