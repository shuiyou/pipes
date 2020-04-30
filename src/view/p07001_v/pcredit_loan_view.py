from mapping.module_processor import ModuleProcessor
from product.date_time_util import before_n_year_date
import numpy as np


class PcreditLoanView(ModuleProcessor):

    def process(self):
        pass


    def _get_loan_msg(self):
        loan_df=self.cached_data.get("pcredit_loan")
        credit_base_info_df = self.cached_data.get("credit_base_info")
        report_time = credit_base_info_df.loc[0, 'report_time']
        report_time_before_2_year = before_n_year_date(report_time, 2)
        if loan_df.empty:
            return
        #经营、消费、按揭类贷款
        loan_account_type_df=loan_df[loan_df['account_type'].isin(['01','02','03'])]
        if not loan_account_type_df.empty:
            #个人信息-固定资产-按揭已归还
            self.variables["mort_settle_loan_date"]=loan_account_type_df[(loan_account_type_df['loan_type'].isin(['03','05','06']))
                                                                         and (loan_account_type_df['loan_status']=='3')].loc[:,'loan_date'].tolist()
            #个人信息-固定资产-按揭未结清
            self.variables["mort_no_settle_loan_date"] = loan_account_type_df[(loan_account_type_df['loan_type'].isin(
                ['03', '05', '06'])) and (loan_account_type_df['loan_status'] != '3')].loc[:, 'loan_date'].tolist()

            report_time_before_2_year_df=loan_account_type_df[(loan_account_type_df['loan_type'].isin(
                ['01', '04', '07', '99'])) and (loan_account_type_df['loan_date'] > report_time_before_2_year)]
            if not report_time_before_2_year_df.empty:
                #信贷交易信息-贷款信息-贷款趋势变化图-发放时间
                self.variables["each_loan_date"]=report_time_before_2_year_df.loc[:, 'loan_date'].tolist()
                #信贷交易信息-贷款信息-贷款趋势变化图-贷款发放额
                self.variables["each_principal_amount"]=report_time_before_2_year_df.loc[:, 'principal_amount'].tolist()
                #信贷交易信息-贷款信息-贷款趋势变化图-贷款类型
                each_loan_type_list=[]
                for index,row in report_time_before_2_year_df:
                    loan_type=row['loan_type']
                    principal_amount=row['principal_amount']
                    if loan_type in ['01','07','99'] or (loan_type=='04' and principal_amount>200000):
                        each_loan_type_list.append('营性贷款')
                    else:
                        each_loan_type_list.append('消费性贷款')
                self.variables["each_loan_type"]=each_loan_type_list
                #信贷交易信息-贷款信息-贷款趋势变化图-账号状态
                self.variables["each_loan_status"]=report_time_before_2_year_df.loc[:,'loan_status'].tolist()
                #信贷交易信息-贷款信息-贷款趋势变化图最大贷款金额
                max_principal_amount=report_time_before_2_year_df.loc[:,'principal_amount;'].max()
                self.variables["max_principal_amount"]=max_principal_amount
                #信贷交易信息-贷款信息-贷款趋势变化图最小贷款金额
                min_principal_amount= report_time_before_2_year_df.loc[:, 'principal_amount;'].min()
                self.variables["min_principal_amount"]=min_principal_amount
                #信贷交易信息-贷款信息-贷款趋势变化图贷款金额极差
                self.variables["rng_principal_amount"]=max_principal_amount-min_principal_amount
                #信贷交易信息-贷款信息-贷款趋势变化图贷款金额比值
                self.variables["multiple_principal_amount"]='%.2f' % (max_principal_amount/min_principal_amount)
                #信贷交易信息-贷款信息-贷款额度区间分布-0-20万笔数
                loan_principal_0_20w_cnt=report_time_before_2_year_df[(report_time_before_2_year_df['principal_amount'] > 0)
                                                                                        and (report_time_before_2_year_df['principal_amount'] <= 200000)].shape[0]
                self.variables["loan_principal_0_20w_cnt"]=loan_principal_0_20w_cnt
                #信贷交易信息-贷款信息-贷款额度区间分布-20-50万笔数
                loan_principal_20_50w_cnt=report_time_before_2_year_df[(report_time_before_2_year_df['principal_amount'] > 200000)
                                             and (report_time_before_2_year_df['principal_amount'] <= 500000)].shape[0]
                self.variables["loan_principal_20_50w_cnt"] = loan_principal_20_50w_cnt
                #信贷交易信息-贷款信息-贷款额度区间分布-50-100万笔数
                loan_principal_50_100w_cnt=report_time_before_2_year_df[(report_time_before_2_year_df['principal_amount'] > 500000)
                                             and (report_time_before_2_year_df['principal_amount'] <= 1000000)].shape[0]
                self.variables["loan_principal_50_100w_cnt"] = loan_principal_50_100w_cnt
                #信贷交易信息-贷款信息-贷款额度区间分布-100-200万笔数
                loan_principal_100_200w_cnt=report_time_before_2_year_df[(report_time_before_2_year_df['principal_amount'] > 1000000)
                                             and (report_time_before_2_year_df['principal_amount'] <= 2000000)].shape[0]
                self.variables["loan_principal_100_200w_cnt"] = loan_principal_100_200w_cnt
                #信贷交易信息-贷款信息-贷款额度区间分布-大于200万笔数
                oan_principal_200w_cnt=report_time_before_2_year_df[(report_time_before_2_year_df['principal_amount'] > 2000000)].shape[0]
                self.variables["oan_principal_200w_cnt"] = oan_principal_200w_cnt
                #信贷交易信息-贷款信息-贷款额度区间分布-总数
                loan_principal_total_cnt=report_time_before_2_year_df[(report_time_before_2_year_df['principal_amount'] > 0)].shape[0]
                self.variables["loan_principal_total_cnt"] = loan_principal_total_cnt
                #信贷交易信息-贷款信息-贷款额度区间分布-0-20万占比
                self.variables["loan_principal_0_20w_prop"]='%.2f' % (loan_principal_0_20w_cnt/loan_principal_total_cnt)
                #信贷交易信息-贷款信息-贷款额度区间分布-20-50万占比
                self.variables["loan_principal_20_50w_prop"] = '%.2f' % (loan_principal_20_50w_cnt / loan_principal_total_cnt)
                #信贷交易信息-贷款信息-贷款额度区间分布-50-100万占比
                self.variables["loan_principal_50_100w_prop"] = '%.2f' % (loan_principal_50_100w_cnt / loan_principal_total_cnt)
                #信贷交易信息-贷款信息-贷款额度区间分布-100-200万占比
                self.variables["loan_principal_100_200w_prop"] = '%.2f' % (loan_principal_100_200w_cnt / loan_principal_total_cnt)
                #信贷交易信息-贷款信息-贷款额度区间分布-大于200万占比
                self.variables["loan_principal_200w_prop"] = '%.2f' % (oan_principal_200w_cnt / loan_principal_total_cnt)

            #信贷交易信息-贷款信息-贷款类型余额分布-贷款类型
            loan_type_list=[]
            #信贷交易信息-贷款信息-贷款类型余额分布-目前余额
            loan_type_balance_list=[]
            #信贷交易信息-贷款信息-贷款类型余额分布-目前笔数
            loan_type_cnt_list=[]
            # 信贷交易信息-贷款信息-贷款类型余额分布-余额占比
            loan_type_balance_prop_list=[]
            loan_busi_df=loan_account_type_df[(loan_account_type_df['loan_type'].isin(['01','07','99'] ))
                                              or ((loan_account_type_df['loan_type']=='04') and (loan_account_type_df['principal_amount']>200000))]
            loan_con_df=loan_account_type_df[(loan_account_type_df['loan_type']=='04') and (loan_account_type_df['principal_amount']<=200000)]
            loan_mor_df=loan_account_type_df[loan_account_type_df['loan_type'].isin(['03','05','06'])]
            loan_busi_balance=0
            loan_con_balance=0
            loan_mor_balance=0
            if not loan_busi_df.empty:
                loan_type_list.append("经营性贷款")
                loan_busi_balance=loan_busi_df.loc[:,'loan_balance'].sum()
                loan_type_balance_list.append(loan_busi_balance)
                loan_type_cnt_list.append(loan_busi_df.shape[0])
            if not loan_con_df.empty:
                loan_type_list.append("消费性贷款")
                loan_con_balance=loan_con_df.loc[:, 'loan_balance'].sum()
                loan_type_balance_list.append(loan_con_balance)
                loan_type_cnt_list.append(loan_con_df.shape[0])
            if not loan_mor_df.empty:
                loan_type_list.append("按揭类贷款")
                loan_mor_balance=loan_mor_df.loc[:, 'loan_balance'].sum()
                loan_type_balance_list.append(loan_mor_balance)
                loan_type_cnt_list.append(loan_mor_df.shape[0])
            loan_total_balance=loan_busi_balance+loan_con_balance+loan_mor_balance
            loan_type_balance_prop_list.append( '%.2f' %(loan_busi_balance/loan_total_balance))
            loan_type_balance_prop_list.append('%.2f' % (loan_con_balance / loan_total_balance))
            loan_type_balance_prop_list.append('%.2f' % (loan_mor_balance / loan_total_balance))

            self.variables["loan_type"]=loan_type_list
            self.variables["loan_type_balance"]=loan_type_balance_list
            self.variables["oan_type_cnt"]=loan_type_cnt_list
            self.variables["loan_type_balance_prop"]=loan_type_balance_prop_list

        #担保类贷款
        loan_gua_df=loan_df[loan_df['account_type']=='06']
        if not loan_gua_df.empty:
            gua_mort_df=loan_gua_df[loan_gua_df['loan_guarantee_type'].isin(['1','2'])]
            gua_credit_df=loan_gua_df[loan_gua_df['loan_guarantee_type'].isin(['3','4','7'])]
            gua_com_df=loan_gua_df[loan_gua_df['loan_guarantee_type'].isin(['5','6'])]
            # 信贷交易信息-贷款信息-担保方式余额分布-担保类型
            guar_type_list = []
            #信贷交易信息-贷款信息-担保方式余额分布-目前余额
            mort_balance=0
            credit_balance=0
            com_balance=0
            guar_type_balance_list=[]
            #信贷交易信息-贷款信息-担保方式余额分布-目前笔数
            guar_type_cnt_list=[]
            #信贷交易信息-贷款信息-担保方式余额分布-余额占比
            guar_type_balance_prop_list=[]
            if not gua_mort_df.empty:
                guar_type_list.append("抵质押类")
                mort_balance=gua_mort_df.loc[:,'loan_balance'].sum()
                guar_type_balance_list.append(mort_balance)
                guar_type_cnt_list.append(gua_mort_df.shape[0])
            if not gua_credit_df.empty:
                guar_type_list.append("担保信用类")
                credit_balance=gua_credit_df.loc[:,'loan_balance'].sum()
                guar_type_balance_list.append(credit_balance)
                guar_type_cnt_list.append(gua_credit_df.shape[0])
            if not gua_com_df.empty:
                guar_type_list.append("组合类")
                com_balance=gua_com_df.loc[:,'loan_balance'].sum()
                guar_type_balance_list.append(com_balance)
                guar_type_cnt_list.append(gua_com_df.shape[0])
            total_balance=mort_balance+credit_balance+com_balance
            if total_balance!=0:
                guar_type_balance_prop_list.append( '%.2f' %(mort_balance/total_balance))
                guar_type_balance_prop_list.append('%.2f' %(credit_balance/total_balance))
                guar_type_balance_prop_list.append('%.2f' % (com_balance / total_balance))
            self.variables["guar_type"]=guar_type_list
            self.variables["guar_type_balance"]=guar_type_balance_list
            self.variables["guar_type_cnt"]=guar_type_cnt_list
            self.variables["guar_type_balance_prop"]=guar_type_balance_prop_list
            #信贷交易信息-贷款信息-担保方式余额分布保证类最大金额
            ensure_max_principal=self._get_one_query_condition_max(loan_gua_df,'loan_guarantee_type',['3','4','7'],'loan_balance','max')
            self.variables["ensure_max_principal"]=ensure_max_principal
            #信贷交易信息-贷款信息-担保方式余额分布抵押类最大金额
            mort_max_principal=self._get_one_query_condition_max(loan_gua_df,'loan_guarantee_type',['1','2'], 'loan_balance','max')
            self.variables["mort_max_principal"] = mort_max_principal

            apply_amount=self.origin_data.get("applyAmount")
            if apply_amount:
                # 信贷交易信息-贷款信息-担保方式余额分布保证类最大金额是我司申请金额倍数
                self.variables["ensure_principal_multi_apply"]='%.2f' %(ensure_max_principal/apply_amount)
                #信贷交易信息-贷款信息-担保方式余额分布抵押类最大金额
                self.variables["mort_principal_multi_apply"] = '%.2f' % (mort_max_principal / apply_amount)




        #征信不良信息-严重预警信息-五级分类状态
        self.variables["category"]=loan_df[(loan_df['account_type'].isin(['01','02','03','04','05']))
                                      and (loan_df['category']!='01')].loc[:,'latest_category'].unique().tolist()



    #单查询条件，获取对应的结果
    def _get_one_query_condition_max(self,df,query_field,query_list,filter_field,method):
        df_temp = df[df[query_field].isin(query_list)]
        if not df_temp.empty:
            if method=='max':
                return  df_temp.loc[:, filter_field].max()
            elif method=='min':
                return df_temp.loc[:, filter_field].min()
            elif method=='sum':
                return df_temp.loc[:, filter_field].sum()
        return 0