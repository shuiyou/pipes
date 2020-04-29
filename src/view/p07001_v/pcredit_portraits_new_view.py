from mapping.module_processor import ModuleProcessor
import pandas as pd
import numpy as np

from product.date_time_util import before_n_month


class PcreditPortraitsNewView(ModuleProcessor):

    def process(self):
        pass

    def _get_portraits_new_msg(self):
        pcredit_loan_df=self.cached_data.get("pcredit_loan")
        credit_base_info_df=self.cached_data.get("credit_base_info")
        report_time=None
        if not credit_base_info_df.empty:
            report_time=credit_base_info_df.loc[0,'report_time']
        if pcredit_loan_df is None or pcredit_loan_df.empty:
            return
        pcredit_loan_df_acc_type=pcredit_loan_df[pcredit_loan_df['account_type'].isin(['01','02','03'])]
        self._get_loan_overdue_money(pcredit_loan_df_acc_type)
        self._get_loan_overdue__cnt(pcredit_loan_df_acc_type,report_time)
        pcredit_loan_df_acc_type1=pcredit_loan_df[pcredit_loan_df['account_type'].isin(['04','05'])]


    def _get_loan_overdue_money(self,df):
        if df.empty:
            return
        self.variables["loan_now_overdue_money"]='%.2f' %(df.loc[:,'overdue_amount'].sum())
        df1=df[(df['loan_type'].isin(['01','07','99'])) or (df['loan_type']=='04' and df['loan_amount']>20000)]
        if not df1.empty:
            self.variables["business_loan_overdue_money"]='%.2f' %(df1.loc[:,'overdue_amount'].sum())

    def _get_loan_overdue__cnt(self,df,report_time):
        if df.empty:
            return
        repayment_df=self.cached_data.get("pcredit_repayment")
        df_temp=df[(df['loan_type'].isin(['01','07','99'])) or (df['loan_type']=='04' and df['loan_amount']>20000)]
        if not df_temp.empty:
            overdue_cnt_df=pd.merge(df_temp,repayment_df,left_on='id',right_on='record_id')
            overdue_cnt_df=overdue_cnt_df[overdue_cnt_df['status']=='1']
            if not overdue_cnt_df.empty:
                self.variables["rhzx_business_loan_overdue_cnt"]=overdue_cnt_df['id'].unique().size

        merge_df=pd.merge(df,repayment_df,left_on='id',right_on='record_id')
        times_cnt_df=merge_df[merge_df['status']=='2']
        if not times_cnt_df.empty:
            self.variables["loan_overdue_2times_cnt"]=times_cnt_df.shape[0]
        #征信不良信息-逾期信息-贷款当前逾期次数
        if report_time is not None:
            year,month=before_n_month(report_time,1)
            now_overdue_cnt_df=merge_df[(merge_df['jhi_year']==year) and (merge_df['month']==month) and (merge_df['status'].str.isdigit()==True)]
            if not now_overdue_cnt_df.empty:
                self.variables["loan_now_overdue_cnt"]=now_overdue_cnt_df.loc[:,'status'].sum()
        #征信不良信息-逾期信息-贷款历史总逾期次数
        total_overdue_cnt_df = merge_df[merge_df['repayment_amt']>0]
        if not total_overdue_cnt_df.empty:
            self.variables["loan_total_overdue_cnt"]=total_overdue_cnt_df.shap[0]
        #征信不良信息-逾期信息-贷款最大连续逾期
        max_overdue_month_df=merge_df[merge_df['status'].str.isdigit()==True]
        if not max_overdue_month_df.empty:
            self.variables["loan_max_overdue_month"]=max_overdue_month_df['status'].max()

    def _get_creidt_overdue_money(self,df):
        if df.empty:
            return
        #征信不良信息-逾期信息-贷记卡当前逾期金额
        self.variables["credit_now_overdue_money"]='%.2f' %(df.loc[:,'overdue_amount'].sum())

    def _get_creidt_overdue_cnt(self,df,report_time):
        if df.empty:
            return
        repayment_df=self.cached_data.get("pcredit_repayment")
        merge_df = pd.merge(df, repayment_df, left_on='id', right_on='record_id')
        #征信不良信息-逾期信息-贷记卡当前逾期次数
        if report_time is not None:
            year,month=before_n_month(report_time,1)
            now_overdue_cnt_df=merge_df[(merge_df['jhi_year']==year) and (merge_df['month']==month) and (merge_df['status'].str.isdigit()==True)]
            if not now_overdue_cnt_df.empty:
                self.variables["credit_now_overdue_cnt"]=now_overdue_cnt_df.loc[:,'status'].sum()
        #征信不良信息-逾期信息-贷记卡历史总逾期次数
        total_overdue_cnt_df = merge_df[merge_df['repayment_amt'] > 0]
        if not total_overdue_cnt_df.empty:
            self.variables["credit_total_overdue_cnt"] = total_overdue_cnt_df.shap[0]
        #征信不良信息-逾期信息-贷记卡最大连续逾期
        max_overdue_month_df = merge_df[merge_df['status'].str.isdigit() == True]
        if not max_overdue_month_df.empty:
            self.variables["credit_overdue_max_month"] = max_overdue_month_df['status'].max()
        #征信不良信息-逾期信息-单张贷记卡2年内逾期次数









