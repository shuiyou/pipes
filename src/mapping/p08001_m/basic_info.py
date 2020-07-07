import pandas as pd
from datetime import datetime
from mapping.trans_module_processor import TransModuleProcessor

class BasicInfoProcessor(TransModuleProcessor):
#保留小数

    def process(self):
        pass


    # 异常交易类 计数
    def _unusual_trans_cnt(self):
        flow_df = self.cached_data['trans_u_flow_portrait']
        # 典当笔数
        pawn = flow_df['unusual_trans_type'].str.contains('典当')
        self.variables['pawn_cnt'] = flow_df['unusual_trans_type'][pawn].count()
        # 医疗笔数
        medical = flow_df['unusual_trans_type'].str.contains('医院')
        self.variables['medical_cnt'] = flow_df['unusual_trans_type'][medical].count()
        # 案件纠纷笔数
        court = flow_df['unusual_trans_type'].str.contains('案件纠纷')
        self.variables['court_cnt'] = flow_df['unusual_trans_type'][court].count()
        # 保险理赔笔数
        insure = flow_df['unusual_trans_type'].str.contains('保险理赔')
        self.variables['insure_cnt'] = flow_df['unusual_trans_type'][insure].count()
        # 夜间交易笔数
        night_trans = flow_df['unusual_trans_type'].str.contains('夜间交易')
        self.variables['night_trans_cnt'] = flow_df['unusual_trans_type'][night_trans].count()
        # 家庭不稳定笔数
        fam_unstab = flow_df['unusual_trans_type'].str.contains('家庭不稳定')
        self.variables['fam_unstab_cnt'] = flow_df['unusual_trans_type'][fam_unstab].count()

    # 余额笔均 余额最大值 余额在0~5万的最大值
    def _balance(self):
        flow_df = self.cached_data['trans_u_flow_portrait']
        balance = flow_df['account_balance']
        self.variables['balance_mean'] = balance.mean()
        self.variables['balance_max'] = balance.max()
        self.variables['balance_max_0_to_5'] = balance[balance<=50000].max()

    # 进账金额   笔均 笔均-1倍标准差 笔均+1倍标准差 笔均-2倍标准差 笔均+2倍标准差
    def _income(self):
        flow_df = self.cached_data['trans_u_flow_portrait']
        trans_amt = flow_df['trans_amt']
        income_mean = trans_amt[trans_amt>0].mean()
        self.variables['income_mean'] = income_mean
        income_std = trans_amt[trans_amt>0].std()
        self.variables['mean_sigma_left'] = income_mean - income_std
        self.variables['mean_sigma_right'] = income_mean + income_std
        self.variables['mean_2_sigma_left'] = income_mean - 2*income_std
        self.variables['mean_2_sigma_right'] = income_mean + 2*income_std

    # 交易对手类
    def _opponent(self):
        flow_df = self.cached_data['trans_u_flow_portrait']
        distinct_opponent_name = flow_df['opponent_name'].drop_duplicates(["opponent_name"],keep = 'first')
        self.variables['opponent_cnt'] = distinct_opponent_name.count()

    # 关联关系类
    def _related(self):
        flow_df = self.cached_data['trans_u_flow_portrait']
        expense_flow = flow_df[flow_df['trans_amt']<0]
        income_flow = flow_df[flow_df['trans_amt']>0]
        self.variables['enterprise_3_income_amt'] = \
            income_flow[income_flow['relationship']=='借款人作为股东的企业']['trans_amt'].sum()
        self.variables['enterprise_3_expense_cnt_prop'] = \
            len(expense_flow[expense_flow['relationship']=='借款人作为股东的企业'])/len(expense_flow)
        self.variables['all_relations_expense_cnt_prop'] = \
            len(expense_flow[pd.notnull(expense_flow['relationship'])])/len(expense_flow)


    # 日常经营类
    def _normal(self):
        flow_df = self.cached_data['trans_u_flow_portrait']
        df = flow_df[(pd.notnull(flow_df['cost_type']))&(flow_df['trans_amt']<0)]
        df['year(trans_date)']  =  df['trans_date'].apply(lambda x: x.year)
        df['month(trans_date)']  = df['trans_date'].apply(lambda  x : x.month)
        self.variables['normal_expense_amt_m_std'] = df.groupby(['year(trans_date)','month(trans_date)'])['trans_amt'].sum().std()


    def _from_u_portrait(self):
        flow_cnt = len(self.cached_data['trans_u_flow_portrait'])
        portrait = self.cached_data['trans_u_portrait']
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
        summary_portrait = self.cached_data['trans_u_summary_portrait']

        self.variables['half_year_interest_amt'] = summary_portrait[summary_portrait['month'] == 'half_year']['interest_amt'].values
        self.variables['half_year_balance_amt'] = summary_portrait[summary_portrait['month'] == 'half_year']['balance_amt'].values
        self.variables['year_interest_amt'] = summary_portrait[summary_portrait['month'] == 'year']['interest_amt'].values
        self.variables['q_2_balance_amt'] = summary_portrait[summary_portrait['month'] == 'quarter2']['balance_amt'].values
        self.variables['q_3_balance_amt'] = summary_portrait[summary_portrait['month'] == 'quarter3']['balance_amt'].values
        self.variables['year_interest_balance_prop'] = summary_portrait[summary_portrait['month'] == 'year']['interest_balance_proportion'].values
        self.variables['q_4_interest_balance_prop'] = summary_portrait[summary_portrait['month'] == 'quarter4']['interest_balance_proportion'].values
        self.variables['income_net_rate_compare_2'] = \
        summary_portrait[summary_portrait.month.isin(['2','3','4'])]['net_income_amt'].sum() \
         / summary_portrait[summary_portrait.month.isin(['5','6','7'])]['net_income_amt'].sum()

    def _from_u_counterparty(self):
        counterparty = self.cached_data['trans_u_counterparty_portrait']


        self.variables['income_rank_1_amt'] = counterparty[(counterparty['income_amt_order']==1)
                                                           &(counterparty['month']=='汇总')]['trans_amt'].values
        self.variables['income_rank_2_amt'] = counterparty[(counterparty['income_amt_order'] == 2)
                                                           & (counterparty['month'] == '汇总')]['trans_amt'].values
        self.variables['income_rank_3_amt'] = counterparty[(counterparty['income_amt_order'] == 3)
                                                           & (counterparty['month'] == '汇总')]['trans_amt'].values
        self.variables['income_rank_4_amt'] = counterparty[(counterparty['income_amt_order'] == 4)
                                                           & (counterparty['month'] == '汇总')]['trans_amt'].values

        self.variables['income_rank_2_cnt_prop'] = counterparty[(counterparty['income_amt_order'] == 2)
                                                                 & (counterparty['month'] == '汇总')]['trans_cnt'].values /\
                                                counterparty[(counterparty['income_amt_order'] == '前100%')
                                                 & (counterparty['month'] == '汇总')]['trans_cnt'].values

        self.variables['expense_rank_6_avg_gap'] = counterparty[(counterparty['expense_amt_order'] == 6)
                                                           & (counterparty['month'] == '汇总')]['trans_gap_avg'].values
        self.variables['income_rank_9_avg_gap'] = counterparty[(counterparty['income_amt_order'] == 9)
                                                                & (counterparty['month'] == '汇总')]['trans_gap_avg'].values
        self.variables['expense_rank_10_avg_gap'] = counterparty[(counterparty['expense_amt_order'] == 10)
                                                                & (counterparty['month'] == '汇总')]['trans_gap_avg'].values

    def _from_u_loan_portrait(self):

        loan_portrait = self.cached_data['trans_u_loan_portrait']
        cm_3 = list(map(str, list(range(1,4))))
        cm_6 = list(map(str,list(range(1,7))))
        cm_12 = list(map(str, list(range(1,13))))

        self.variables['hit_loan_type_cnt_6_cm'] =  loan_portrait[loan_portrait.month.isin(cm_6)]['loan_type'].nunique()

        self.variables['private_income_amt_12_cm'] =   loan_portrait[(loan_portrait.month.isin(cm_12))
                                                &(loan_portrait['loan_type'] == '民间借贷')]['loan_amt'].sum()
        self.variables['private_income_mean_12_cm'] = loan_portrait[(loan_portrait.month.isin(cm_12))
                                                    & (loan_portrait['loan_type'] == '民间借贷')]['loan_amt'].sum() / \
                                                      loan_portrait[(loan_portrait.month.isin(cm_12))
                                                    & (loan_portrait['loan_type'] == '民间借贷')]['loan_cnt'].sum()

        self.variables['pettyloan_income_amt_12_cm'] = loan_portrait[(loan_portrait.month.isin(cm_12))
                                                &(loan_portrait['loan_type'] == '小贷')]['loan_amt'].sum()
        self.variables['pettyloan_income_mean_12_cm'] = loan_portrait[(loan_portrait.month.isin(cm_12))
                                                        & (loan_portrait['loan_type'] == '小贷')]['loan_amt'].sum() / \
                                                        loan_portrait[(loan_portrait.month.isin(cm_12))
                                                    & (loan_portrait['loan_type'] == '小贷')]['loan_cnt'].sum()

        self.variables['finlease_expense_cnt_6_cm'] = loan_portrait[(loan_portrait.month.isin(cm_6))
                                                &(loan_portrait['loan_type'] == '融资租赁')]['repay_cnt'].sum()

        self.variables['otherfin_income_mean_3_cm'] = loan_portrait[(loan_portrait.month.isin(cm_3))
                                                        & (loan_portrait['loan_type'] == '其他金融')]['loan_amt'].sum() / \
                                                        loan_portrait[(loan_portrait.month.isin(cm_3))
                                                    & (loan_portrait['loan_type'] == '其他金融')]['loan_cnt'].sum()

        self.variables['all_loan_expense_cnt_3_cm'] = loan_portrait[loan_portrait.month.isin(cm_3)]['repay_cnt'].sum()

    def _from_u_related_portrait(self):
        related_portrait = self.cached_data['trans_u_related_portrait']
        self.variables['enterprise_3_income_amt'] = related_portrait[related_portrait['relationship']=='借款人作为股东的企业']['income_amt'].values()



    def _prediction(self):

        def output(q_2_balance_amt,
                   balance_max_0_to_5,
                   mean_sigma_left,
                   mean_sigma_right,
                   q_3_balance_amt,
                   income_max_weight,
                   mean_2_sigma_left,
                   balance_max,
                   balance_min_weight,
                   balance_max_weight):
            q_2_balance_amt = max(min(q_2_balance_amt,195859.5),-107808.77)
            balance_max_0_to_5 = max(min(balance_max_0_to_5,54224.48),41950.83)
            mean_sigma_left = max(min(mean_sigma_left,62770.26),-125197.66)
            mean_sigma_right = max(min(mean_sigma_right,390882.77),-159093.06)
            q_3_balance_amt = max(min(q_3_balance_amt,165827.93),-93053.75)
            income_max_weight = max(min(income_max_weight,173475.71),-18316.07)
            mean_2_sigma_left = max(min(mean_2_sigma_left, 178849.4), -397759.54)
            balance_max = max(min(balance_max, 2363684.4), -1074526.9)
            balance_min_weight = max(min(balance_min_weight, 184786.17), -98508.38)
            balance_max_weight = max(min(balance_max_weight, 351823.87), -121134.98)

            cus_apply_amt_pred = q_2_balance_amt * 0.4093 +\
                                balance_max_0_to_5 * 8.5719 +\
                                mean_sigma_left * 1.4890 +\
                                mean_sigma_right * 2.7299 +\
                                q_3_balance_amt * 0.3622 +\
                                income_max_weight * (-2.6788) +\
                                mean_2_sigma_left * 0.8685 +\
                                balance_max * 0.0902 +\
                                balance_min_weight * 2.3637 +\
                                balance_max_weight * (-1.2959)

            if cus_apply_amt_pred < 0 :
                return 0
            elif cus_apply_amt_pred > 3000000:
                return 3000000

        self.variables["cus_apply_amt_pred"] = output(
                                    self.variables["q_2_balance_amt"],
                                    self.variables["balance_max_0_to_5"],
                                    self.variables["mean_sigma_left"],
                                    self.variables["mean_sigma_right"],
                                    self.variables["q_3_balance_amt"],
                                    self.variables["income_max_weight"],
                                    self.variables["mean_2_sigma_left"],
                                    self.variables["balance_max"],
                                    self.variables["balance_min_weight"],
                                    self.variables["balance_max_weight"]
                                    )