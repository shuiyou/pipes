import pandas as pd
from datetime import datetime
from util.mysql_reader import sql_to_df
from mapping.trans_module_processor import TransModuleProcessor


class GetVariableInDB(TransModuleProcessor):

    def process(self):
        self._from_u_portrait()
        self._from_u_summary_portrait()
        self._from_u_counterparty()
        self._from_u_loan_portrait()
        self._from_u_related_portrait()

    def _from_u_portrait(self):
        flow_cnt = len(self.trans_u_flow_portrait)
        sql = """
            select *
            from trans_u_portrait
            where report_req_no = %(report_req_no)s
        """
        portrait = sql_to_df(sql=sql,
                             params={"report_req_no": self.reqno})
        self.variables['balance_0_to_5_prop'] = portrait['balance_0_to_5_day'].values / flow_cnt
        self.variables['income_0_to_5_prop'] = portrait['income_0_to_5_cnt'].values / flow_cnt
        self.variables['balance_min_weight'] = portrait['balance_weight_min'].values
        self.variables['balance_max_weight'] = portrait['balance_weight_max'].values
        self.variables['income_max_weight'] = portrait['income_weight_max'].values
        self.variables['normal_income_mean'] = portrait['normal_income_mean'].values
        self.variables['normal_income_amt_d_mean'] = portrait['normal_income_amt_d_mean'].values
        self.variables['normal_income_amt_m_mean'] = portrait['normal_income_amt_m_mean'].values
        self.variables['relationship_risk'] = portrait['relationship_risk'].values

    def _from_u_summary_portrait(self):
        sql = """
            select *
            from trans_u_summary_portrait
            where report_req_no = %(report_req_no)s
        """
        summary_portrait = sql_to_df(sql=sql,
                                     params={"report_req_no": self.reqno})
        self.variables['half_year_interest_amt'] = summary_portrait[summary_portrait['month'] == 'half_year'][
            'interest_amt'].values
        self.variables['half_year_balance_amt'] = summary_portrait[summary_portrait['month'] == 'half_year'][
            'balance_amt'].values
        self.variables['year_interest_amt'] = summary_portrait[summary_portrait['month'] == 'year'][
            'interest_amt'].values
        self.variables['q_2_balance_amt'] = summary_portrait[summary_portrait['month'] == 'quarter2'][
            'balance_amt'].values
        self.variables['q_3_balance_amt'] = summary_portrait[summary_portrait['month'] == 'quarter3'][
            'balance_amt'].values
        self.variables['year_interest_balance_prop'] = summary_portrait[summary_portrait['month'] == 'year'][
            'interest_balance_proportion'].values
        self.variables['q_4_interest_balance_prop'] = summary_portrait[summary_portrait['month'] == 'quarter4'][
            'interest_balance_proportion'].values
        self.variables['income_net_rate_compare_2'] = \
            summary_portrait[summary_portrait.month.isin(['2', '3', '4'])]['net_income_amt'].sum() \
            / summary_portrait[summary_portrait.month.isin(['5', '6', '7'])]['net_income_amt'].sum()

    def _from_u_counterparty(self):
        sql = """
            select *
            from trans_u_counterparty_portrait
            where report_req_no = %(report_req_no)s
        """
        counterparty = sql_to_df(sql=sql,
                                 params={"report_req_no": self.reqno})
        self.variables['income_rank_1_amt'] = counterparty[(counterparty['income_amt_order'] == 1)
                                                           & (counterparty['month'] == '汇总')]['trans_amt'].values
        self.variables['income_rank_2_amt'] = counterparty[(counterparty['income_amt_order'] == 2)
                                                           & (counterparty['month'] == '汇总')]['trans_amt'].values
        self.variables['income_rank_3_amt'] = counterparty[(counterparty['income_amt_order'] == 3)
                                                           & (counterparty['month'] == '汇总')]['trans_amt'].values
        self.variables['income_rank_4_amt'] = counterparty[(counterparty['income_amt_order'] == 4)
                                                           & (counterparty['month'] == '汇总')]['trans_amt'].values

        self.variables['income_rank_2_cnt_prop'] = counterparty[(counterparty['income_amt_order'] == 2)
                                                                & (counterparty['month'] == '汇总')]['trans_cnt'].values / \
                                                   counterparty[(counterparty['income_amt_order'] == '前100%')
                                                                & (counterparty['month'] == '汇总')]['trans_cnt'].values

        self.variables['expense_rank_6_avg_gap'] = counterparty[(counterparty['expense_amt_order'] == 6)
                                                                & (counterparty['month'] == '汇总')][
            'trans_gap_avg'].values
        self.variables['income_rank_9_avg_gap'] = counterparty[(counterparty['income_amt_order'] == 9)
                                                               & (counterparty['month'] == '汇总')][
            'trans_gap_avg'].values
        self.variables['expense_rank_10_avg_gap'] = counterparty[(counterparty['expense_amt_order'] == 10)
                                                                 & (counterparty['month'] == '汇总')][
            'trans_gap_avg'].values

    def _from_u_loan_portrait(self):
        sql = """
            select *
            from trans_u_loan_portrait
            where report_req_no = %(report_req_no)s
        """
        loan_portrait = sql_to_df(sql=sql,
                                  params={"report_req_no": self.reqno})
        cm_3 = list(map(str, list(range(1, 4))))
        cm_6 = list(map(str, list(range(1, 7))))
        cm_12 = list(map(str, list(range(1, 13))))

        self.variables['hit_loan_type_cnt_6_cm'] = loan_portrait[loan_portrait.month.isin(cm_6)]['loan_type'].nunique()

        self.variables['private_income_amt_12_cm'] = loan_portrait[(loan_portrait.month.isin(cm_12))
                                                                   & (loan_portrait['loan_type'] == '民间借贷')][
            'loan_amt'].sum()
        self.variables['private_income_mean_12_cm'] = loan_portrait[(loan_portrait.month.isin(cm_12))
                                                                    & (loan_portrait['loan_type'] == '民间借贷')][
                                                          'loan_amt'].sum() / \
                                                      loan_portrait[(loan_portrait.month.isin(cm_12))
                                                                    & (loan_portrait['loan_type'] == '民间借贷')][
                                                          'loan_cnt'].sum()

        self.variables['pettyloan_income_amt_12_cm'] = loan_portrait[(loan_portrait.month.isin(cm_12))
                                                                     & (loan_portrait['loan_type'] == '小贷')][
            'loan_amt'].sum()
        self.variables['pettyloan_income_mean_12_cm'] = loan_portrait[(loan_portrait.month.isin(cm_12))
                                                                      & (loan_portrait['loan_type'] == '小贷')][
                                                            'loan_amt'].sum() / \
                                                        loan_portrait[(loan_portrait.month.isin(cm_12))
                                                                      & (loan_portrait['loan_type'] == '小贷')][
                                                            'loan_cnt'].sum()

        self.variables['finlease_expense_cnt_6_cm'] = loan_portrait[(loan_portrait.month.isin(cm_6))
                                                                    & (loan_portrait['loan_type'] == '融资租赁')][
            'repay_cnt'].sum()

        self.variables['otherfin_income_mean_3_cm'] = loan_portrait[(loan_portrait.month.isin(cm_3))
                                                                    & (loan_portrait['loan_type'] == '其他金融')][
                                                          'loan_amt'].sum() / \
                                                      loan_portrait[(loan_portrait.month.isin(cm_3))
                                                                    & (loan_portrait['loan_type'] == '其他金融')][
                                                          'loan_cnt'].sum()

        self.variables['all_loan_expense_cnt_3_cm'] = loan_portrait[loan_portrait.month.isin(cm_3)]['repay_cnt'].sum()

    def _from_u_related_portrait(self):
        sql = """
            select *
            from trans_u_related_portrait
            where report_req_no = %(report_req_no)s
        """
        related_portrait = sql_to_df(sql=sql,
                                     params={"report_req_no": self.reqno})
        self.variables['enterprise_3_income_amt'] = related_portrait[related_portrait['relationship'] == '借款人作为股东的企业'][
            'income_amt'].values()
