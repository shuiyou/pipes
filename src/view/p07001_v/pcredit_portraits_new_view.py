from mapping.module_processor import ModuleProcessor
import pandas as pd
import numpy as np


class PcreditPortraitsNewView(ModuleProcessor):

    def process(self):
        pass

    def _get_portraits_new_msg(self):
        pcredit_loan_df=self.cached_data.get("pcredit_loan")
        if pcredit_loan_df is None or pcredit_loan_df.empty:
            return
        pcredit_loan_df_acc_type=pcredit_loan_df[pcredit_loan_df['account_type'].isin(['01','02','03'])]


    def _get_loan_now_overdue_money(self,df):
        if df.empty:
            return
        self.variables["loan_now_overdue_money"]='%.2f' %(df.loc[:,'overdue_amount'].sum())
        df1=df[(df['loan_type'].isin(['01','07','99'])) or (df['loan_type']=='04' and df['loan_amount']>20000)]
        if not df1.empty:
            self.variables["business_loan_overdue_money"]='%.2f' %(df1.loc[:,'overdue_amount'].sum())

    def _get_rhzx_business_loan_overdue_cnt(self,df):
        if df.empty:
            return
        repayment_df=self.cached_data.get("pcredit_repayment")
        df_temp=df[(df['loan_type'].isin(['01','07','99'])) or (df['loan_type']=='04' and df['loan_amount']>20000)]
        if not df_temp.empty:
            overdue_cnt_df=pd.merge(df_temp,repayment_df,left_on='id',right_on='record_id')
            overdue_cnt_df=overdue_cnt_df[overdue_cnt_df['status']=='1']
            if not overdue_cnt_df.empty:
                self.variables["rhzx_business_loan_overdue_cnt"]=len(set(overdue_cnt_df.loc[:,'id'].tolist()))

        merge_df=pd.merge(df,repayment_df,left_on='id',right_on='record_id')
        times_cnt_df=merge_df[merge_df['status']=='2']
        if not times_cnt_df.empty:
            self.variables["loan_overdue_2times_cnt"]=times_cnt_df.shape[0]



        total_overdue_cnt_df = merge_df[merge_df['repayment_amt']>0]
        if not total_overdue_cnt_df.empty:
            self.variables["loan_total_overdue_cnt"]=total_overdue_cnt_df.shap[0]











