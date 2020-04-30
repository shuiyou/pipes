from mapping.module_processor import ModuleProcessor
from product.date_time_util import before_n_year_date


class PcreditLoanView(ModuleProcessor):

    def process(self):
        pass


    def _get_loan_msg(self):
        loan_df=self.cached_data.get("pcredit_loan")
        credit_base_info_df = self.cached_data.get("credit_base_info")
        report_time = credit_base_info_df.loc[0, 'report_time']
        report_time_before_2_year = before_n_year_date(report_time, 2)
        if loan_df is None or loan_df.empty:
            return
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



        #征信不良信息-严重预警信息-五级分类状态
        self.variables["category"]=loan_df[(loan_df['account_type'].isin(['01','02','03','04','05']))
                                      and (loan_df['category']!='01')].loc[:,'latest_category'].unique().tolist()
