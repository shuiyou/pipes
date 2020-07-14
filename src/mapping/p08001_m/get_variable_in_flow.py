import pandas as pd
from datetime import datetime

from mapping.utils.df_utils import nan_to_zero
from util.mysql_reader import sql_to_df
from mapping.trans_module_processor import TransModuleProcessor


class GetVariableInFlow(TransModuleProcessor):

    def process(self):
        self._unusual_trans_cnt()
        self._balance()
        self._income()
        self._opponent()
        self._related()
        self._normal()

    # 异常交易类 计数
    def _unusual_trans_cnt(self):
        flow_df = self.trans_u_flow_portrait
        # 典当笔数
        self.variables['pawn_cnt'] = flow_df['unusual_trans_type'].str.contains('典当').fillna(False).sum()
        # 医疗笔数
        self.variables['medical_cnt'] = flow_df['unusual_trans_type'].str.contains('医院').fillna(False).sum()
        # 案件纠纷笔数
        self.variables['court_cnt'] = flow_df['unusual_trans_type'].str.contains('案件纠纷').fillna(False).sum()
        # 保险理赔笔数
        self.variables['insure_cnt'] = flow_df['unusual_trans_type'].str.contains('保险理赔').fillna(False).sum()
        # 夜间交易笔数
        self.variables['night_trans_cnt'] = flow_df['unusual_trans_type'].str.contains('夜间交易').fillna(False).sum()
        # 家庭不稳定笔数
        self.variables['fam_unstab_cnt'] = flow_df['unusual_trans_type'].str.contains('家庭不稳定').fillna(False).sum()

    # 余额笔均 余额最大值 余额在0~5万的最大值
    def _balance(self):
        flow_df = self.trans_u_flow_portrait
        balance = flow_df['account_balance']
        self.variables['balance_mean'] = round(nan_to_zero(balance.mean()),4)
        self.variables['balance_max'] = nan_to_zero(balance.max())
        self.variables['balance_max_0_to_5'] = nan_to_zero(balance[balance <= 50000].max())

    # 进账金额   笔均 笔均-1倍标准差 笔均+1倍标准差 笔均-2倍标准差 笔均+2倍标准差
    def _income(self):
        flow_df = self.trans_u_flow_portrait
        trans_amt = flow_df['trans_amt']
        income_mean = nan_to_zero(trans_amt[trans_amt > 0].mean())
        self.variables['income_mean'] = round(income_mean,4)
        income_std = nan_to_zero(trans_amt[trans_amt > 0].std())
        self.variables['mean_sigma_left'] = round(income_mean - income_std,4)
        self.variables['mean_sigma_right'] = round(income_mean + income_std,4)
        self.variables['mean_2_sigma_left'] = round(income_mean - 2 * income_std,4)
        self.variables['mean_2_sigma_right'] = round(income_mean + 2 * income_std,4)

    # 交易对手类
    def _opponent(self):
        flow_df = self.trans_u_flow_portrait
        self.variables['opponent_cnt'] = flow_df[pd.notnull(flow_df.opponent_name)]['opponent_name'].nunique()

    # 关联关系类
    def _related(self):
        flow_df = self.trans_u_flow_portrait
        expense_flow = flow_df[flow_df['trans_amt'] < 0]
        income_flow = flow_df[flow_df['trans_amt'] > 0]
        self.variables['enterprise_3_income_amt'] = round(
            income_flow[income_flow['relationship'] == '借款人作为股东的企业']['trans_amt'].sum(),4)
        if len(expense_flow) > 0:
            self.variables['enterprise_3_expense_cnt_prop'] = round(
                len(expense_flow[expense_flow['relationship'] == '借款人作为股东的企业']) / len(expense_flow),4)
            self.variables['all_relations_expense_cnt_prop'] = round(
                len(expense_flow[pd.notnull(expense_flow['relationship'])]) / len(expense_flow),4)

    # 日常经营类
    def _normal(self):
        flow_df = self.trans_u_flow_portrait
        df = flow_df[(pd.isnull(flow_df.relationship)) & (flow_df['trans_amt'] < 0) &(flow_df.is_sensitive != 1)]
        df['year(trans_date)'] = df['trans_date'].apply(lambda x: x.year)
        df['month(trans_date)'] = df['trans_date'].apply(lambda x: x.month)
        self.variables['normal_expense_amt_m_std'] = round(nan_to_zero(df.groupby(['year(trans_date)', 'month(trans_date)'])[
            'trans_amt'].sum().std()),4)