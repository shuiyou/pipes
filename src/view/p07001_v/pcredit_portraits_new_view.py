from mapping.module_processor import ModuleProcessor
import pandas as pd
import numpy as np

from product.date_time_util import before_n_month, before_n_year


class PcreditPortraitsNewView(ModuleProcessor):

    def process(self):
        self._get_portraits_new_msg()

    def _get_portraits_new_msg(self):
        pcredit_loan_df=self.cached_data.get("pcredit_loan")
        credit_base_info_df=self.cached_data.get("credit_base_info")
        pcredit_info_df=self.cached_data.get("pcredit_info")
        report_time=None
        if not credit_base_info_df.empty:
            report_time=credit_base_info_df.loc[0,'report_time']
        if pcredit_loan_df is not None and not pcredit_loan_df.empty:
            pcredit_loan_df_acc_type = pcredit_loan_df[pcredit_loan_df['account_type'].isin(['01', '02', '03'])]
            self._get_loan_overdue_money(pcredit_loan_df_acc_type)
            self._get_loan_overdue__cnt(pcredit_loan_df_acc_type, report_time)
            pcredit_crdit_df_acc_type = pcredit_loan_df[pcredit_loan_df['account_type'].isin(['04', '05'])]
            self._get_creidt_overdue_money(pcredit_crdit_df_acc_type)
            self._get_creidt_overdue_cnt(pcredit_crdit_df_acc_type, report_time)

        if pcredit_info_df is not None and not pcredit_info_df.empty:
            self._get_pcredit_info_money(pcredit_info_df)



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

        if report_time is not None:
            # 征信不良信息-逾期信息-贷款当前逾期次数
            year,month=before_n_month(report_time,1)
            now_overdue_cnt_df=merge_df[(merge_df['jhi_year']==year) and (merge_df['month']==month) and (merge_df['status'].str.isdigit()==True)]
            if not now_overdue_cnt_df.empty:
                self.variables["loan_now_overdue_cnt"]=now_overdue_cnt_df.shape[0]

            # 征信不良信息-逾期信息-单笔房贷2年内逾期次数
            merge_df_overdue_cnt_2y=self._util_filter_and_merge_df(df,repayment_df,['03','05','06'])
            overdue_cnt_2y_df=self._util_filter_n_year_df(merge_df_overdue_cnt_2y,report_time,2)
            if not overdue_cnt_2y_df.empty:
                self.variables["single_house_loan_overdue_cnt_2y"] = overdue_cnt_2y_df.groupby('id').size().max()

            #征信不良信息-逾期信息-单笔车贷2年内逾期次数
            car_loan_overdue_cnt_2y_merge=self._util_filter_and_merge_df(df,repayment_df,['02'])
            car_loan_overdue_cnt_2y_df=self._util_filter_n_year_df(car_loan_overdue_cnt_2y_merge,report_time,2)
            if not car_loan_overdue_cnt_2y_df.empty:
                self.variables["single_car_loan_overdue_cnt_2y"]=car_loan_overdue_cnt_2y_df.groupby('id').size().max()

            #征信不良信息-逾期信息-单笔消费贷2年内逾期次数
            consume_loan_overdue_cnt_2y_merge=self._util_filter_and_merge_df(df,repayment_df,['04'])
            consume_loan_overdue_cnt_2y_df=self._util_filter_n_year_df(consume_loan_overdue_cnt_2y_merge,report_time,2)
            if not consume_loan_overdue_cnt_2y_df.empty:
                self.variables["single_consume_loan_overdue_cnt_2y"]=consume_loan_overdue_cnt_2y_df.groupby('id').size().max()

            #征信不良信息-逾期信息-消费贷5年内总逾期次数
            consume_loan_overdue_cnt_5y_merge = self._util_filter_and_merge_df(df, repayment_df, ['02','03','04','05','06'])
            consume_loan_overdue_cnt_5y_df = self._util_filter_n_year_df(consume_loan_overdue_cnt_5y_merge, report_time, 5)
            if not consume_loan_overdue_cnt_5y_df.empty:
                self.variables["total_consume_loan_overdue_cnt_5y"] = consume_loan_overdue_cnt_5y_df.shape[0]

                #征信不良信息-逾期信息-消费贷5年内总逾期金额
                self.variables["total_consume_loan_overdue_money_5y"] = self._util_get_total_consume_loan_overdue_money_5y(
                    consume_loan_overdue_cnt_5y_df)







        #征信不良信息-逾期信息-贷款历史总逾期次数
        total_overdue_cnt_df = merge_df[merge_df['repayment_amt']>0]
        if not total_overdue_cnt_df.empty:
            self.variables["loan_total_overdue_cnt"]=total_overdue_cnt_df.shap[0]

        #征信不良信息-逾期信息-贷款最大连续逾期
        max_overdue_month_df=merge_df[merge_df['status'].str.isdigit()==True]
        if not max_overdue_month_df.empty:
            self.variables["loan_max_overdue_month"]=max_overdue_month_df['status'].apply(int).max()

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
                self.variables["credit_now_overdue_cnt"]=now_overdue_cnt_df.shape[0]
        #征信不良信息-逾期信息-贷记卡历史总逾期次数
        total_overdue_cnt_df = merge_df[merge_df['repayment_amt'] > 0]
        if not total_overdue_cnt_df.empty:
            self.variables["credit_total_overdue_cnt"] = total_overdue_cnt_df.shape[0]
        #征信不良信息-逾期信息-贷记卡最大连续逾期
        max_overdue_month_df = merge_df[merge_df['status'].str.isdigit() == True]
        if not max_overdue_month_df.empty:
            self.variables["credit_overdue_max_month"] = max_overdue_month_df['status'].apply(int).max()
        #征信不良信息-逾期信息-单张贷记卡2年内逾期次数
        if report_time is not None:
            overdue_cnt_2y_df=self._util_filter_n_year_df(merge_df,report_time,2)
            if not overdue_cnt_2y_df.empty:
                self.variables["single_credit_overdue_cnt_2y"]=overdue_cnt_2y_df.groupby('id').size().max()

    def _get_pcredit_info_money(self,df):
        #信贷交易信息-资金压力解析-银行授信总额
        self.variables["total_bank_credit_limit"]='%.2f' % df.loc[0,['nonRevolloan_totalcredit','revolcredit_totalcredit','revolloan_totalcredit','undestroy_limit','undestory_semi_limit']].sum()

        #信贷交易信息-资金压力解析-银行总余额
        self.variables["total_bank_loan_balance"]='%.2f' % df.loc[0,['nonRevolloan_balance','revolcredit_balance','revolloan_balance','undestroy_used_limit','undestory_semi_overdraft']].sum()



    def _util_filter_n_year_df(self,df,report_time,n):
        year_2, month_2 = before_n_year(report_time, n)
        resp_df = df[
            ((df['jhi_year'] > year_2) or (df['jhi_year'] == year_2 and df['month'] <= month_2)) and (
                    df['status'].str.isdigit() == True)]
        return resp_df

    def _util_filter_and_merge_df(self,df,target_df,filter_param_list):
        if filter_param_list is not None:
            df_temp = df[df['loan_type'].isin(filter_param_list)]
            df_merge = pd.merge(df_temp, target_df, left_on='id', right_on='record_id')
        else:
            df_merge = pd.merge(df, target_df, left_on='id', right_on='record_id')
        return df_merge

    def _util_get_total_consume_loan_overdue_money_5y(self,raw_df):
        total_money_list = []
        df_group = raw_df.groupby('id')
        for key, df in df_group:
            list_repayment_amt = []
            list_status = []
            for index, row in df.iterrows():
                status = row['status']
                repayment_amt = row['repayment_amt']
                if status == '1' and len(list_status) > 1:
                    total_money_list.append(list_repayment_amt[-1])
                if status == '1':
                    list_repayment_amt.clear()
                    list_status.clear()
                list_status.append(status)
                list_repayment_amt.append(repayment_amt)
        if len(total_money_list) > 0:
            return  '%.2f' % sum(total_money_list)
        return 0








