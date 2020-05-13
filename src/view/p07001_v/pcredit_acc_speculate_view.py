from mapping.module_processor import ModuleProcessor
from product.date_time_util import before_n_month_date, after_n_month_date, before_n_year_date
import pandas as pd


class PcreditAccSpeculateView(ModuleProcessor):

    def process(self):
        pass

    def _get_pcredit_acc_speculate(self):
        pcredit_loan_df=self.cached_data.get("pcredit_loan")
        pcredit_acc_speculate_df=self.cached_data.get["pcredit_acc_speculate"]
        pcredit_acc_speculate_df=self.acc_speculate_df_time_format(pcredit_acc_speculate_df)
        credit_base_info_df = self.cached_data.get("credit_base_info")
        pcredit_info_df = self.cached_data.get("pcredit_info")

        report_time = credit_base_info_df.loc[0, 'report_time']
        report_time_before_6_month=before_n_month_date(report_time,6)
        report_time_before_5_month = before_n_month_date(report_time, 5)
        report_time_before_4_month = before_n_month_date(report_time, 4)
        report_time_before_3_month = before_n_month_date(report_time, 3)
        report_time_before_2_month = before_n_month_date(report_time, 2)
        report_time_before_1_month = before_n_month_date(report_time, 1)
        report_time_after_1_month = after_n_month_date(report_time, 1)
        report_time_after_2_month = after_n_month_date(report_time, 2)
        report_time_after_3_month = after_n_month_date(report_time, 3)
        report_time_after_4_month = after_n_month_date(report_time, 4)
        report_time_after_5_month = after_n_month_date(report_time, 5)
        report_time_after_6_month = after_n_month_date(report_time, 6)
        report_time_after_7_month = after_n_month_date(report_time, 7)
        report_time_after_8_month = after_n_month_date(report_time, 8)
        report_time_after_9_month = after_n_month_date(report_time, 9)
        report_time_after_10_month = after_n_month_date(report_time, 10)
        report_time_after_11_month = after_n_month_date(report_time, 11)
        report_time_before_4_year = before_n_year_date(report_time, 4)
        report_time_before_3_year = before_n_year_date(report_time, 3)
        report_time_before_2_year = before_n_year_date(report_time, 2)
        report_time_before_1_year = before_n_year_date(report_time, 1)
        report_time_before_0_year = report_time


        loan_df_account_type_01_05=pcredit_loan_df[pcredit_loan_df['account_type'].isin(['01','02','03','04','05'])]
        loan_df_account_type_01_03=pcredit_loan_df[pcredit_loan_df['account_type'].isin(['01','02','03'])]
        loan_df_account_type_04_05 = pcredit_loan_df[pcredit_loan_df['account_type'].isin(['04','05'])]

        if not loan_df_account_type_01_05.empty:
            loan_df_account_type_01_05=pd.merge(loan_df_account_type_01_05,pcredit_acc_speculate_df,left_on='id',right_on='record_id')
            #信贷交易信息-资金压力解析-应还总额6个月前
            total_repay_6m_before = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                                         report_time_before_6_month)
            self.variables["total_repay_6m_before"] = total_repay_6m_before
            #信贷交易信息-资金压力解析-应还总额5个月前
            total_repay_5m_before = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                                         report_time_before_6_month)
            self.variables["total_repay_5m_before"] = total_repay_5m_before
            #信贷交易信息-资金压力解析-应还总额4个月前
            total_repay_4m_before = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                                         report_time_before_4_month)
            self.variables["total_repay_4m_before"] = total_repay_4m_before
            #信贷交易信息-资金压力解析-应还总额3个月前
            total_repay_3m_before = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                                         report_time_before_3_month)
            self.variables["total_repay_3m_before"] = total_repay_3m_before
            #信贷交易信息-资金压力解析-应还总额2个月前
            total_repay_2m_before = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                                         report_time_before_2_month)
            self.variables["total_repay_2m_before"] = total_repay_2m_before
            #信贷交易信息-资金压力解析-应还总额1个月前
            total_repay_1m_before = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                                         report_time_before_1_month)
            self.variables["total_repay_1m_before"] = total_repay_1m_before
            #信贷交易信息-资金压力解析-应还总额1个月后
            total_repay_1m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                                         report_time)
            self.variables["total_repay_1m_after"] = total_repay_1m_after
            #信贷交易信息-资金压力解析-应还总额2个月后
            total_repay_2m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                                        report_time_after_1_month)
            self.variables["total_repay_2m_after"] = total_repay_2m_after
            #信贷交易信息-资金压力解析-应还总额3个月后
            total_repay_3m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                                        report_time_after_2_month)
            self.variables["total_repay_3m_after"] = total_repay_3m_after
            #信贷交易信息-资金压力解析-应还总额4个月后
            total_repay_4m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                                        report_time_after_3_month)
            self.variables["total_repay_4m_after"] = total_repay_4m_after
            #信贷交易信息-资金压力解析-应还总额5个月后
            total_repay_5m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                                        report_time_after_4_month)
            self.variables["total_repay_5m_after"] = total_repay_5m_after
            # 信贷交易信息-资金压力解析-应还总额6个月后
            total_repay_6m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                                        report_time_after_5_month)
            self.variables["total_repay_6m_after"] = total_repay_6m_after
            # 信贷交易信息-资金压力解析-应还总额7个月后
            total_repay_7m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                                        report_time_after_6_month)
            self.variables["total_repay_7m_after"] = total_repay_7m_after
            # 信贷交易信息-资金压力解析-应还总额8个月后
            total_repay_8m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                                        report_time_after_7_month)
            self.variables["total_repay_8m_after"] = total_repay_8m_after
            # 信贷交易信息-资金压力解析-应还总额9个月后
            total_repay_9m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                                        report_time_after_8_month)
            self.variables["total_repay_9m_after"] = total_repay_9m_after
            # 信贷交易信息-资金压力解析-应还总额10个月后
            total_repay_10m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                                        report_time_after_9_month)
            self.variables["total_repay_10m_after"] = total_repay_10m_after
            # 信贷交易信息-资金压力解析-应还总额11个月后
            total_repay_11m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                                        report_time_after_10_month)
            self.variables["total_repay_11m_after"] = total_repay_11m_after
            # 信贷交易信息-资金压力解析-应还总额12个月后
            total_repay_12m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                                        report_time_after_11_month)
            self.variables["total_repay_12m_after"] = total_repay_12m_after
            #信贷交易信息-资金压力解析-过去6个月平均应还款
            self.variables["average_repay_6m_before"] = '%.2f' %(total_repay_6m_before+total_repay_5m_before+total_repay_4m_before+total_repay_3m_before+total_repay_2m_before+total_repay_1m_before)/6
            #信贷交易信息-资金压力解析-未来12个月平均应还款
            self.variables["average_repay_12m_after"] = '%.2f' %(total_repay_1m_after+total_repay_2m_after+total_repay_3m_after+total_repay_4m_after+total_repay_5m_after+total_repay_6m_after+total_repay_7m_after+
                                                                 total_repay_8m_after+total_repay_9m_after+total_repay_10m_after+total_repay_11m_after+total_repay_12m_after)/12


        if not loan_df_account_type_01_03.empty:
            loan_df_account_type_01_03=pd.merge(loan_df_account_type_01_03,pcredit_acc_speculate_df,left_on='id',right_on='record_id')
            #信贷交易信息-资金压力解析-应还贷款总额6个月前
            self.variables["repay_loan_6m_before"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                        report_time_before_6_month)
            # 信贷交易信息-资金压力解析-应还本金6个月前
            self.variables["repay_principal_6m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_6_month,'loan_repay_type', ['13'])
            # 信贷交易信息-资金压力解析-应还分期额6个月前
            self.variables["repay_installment_6m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_6_month, 'loan_repay_type', ['11', '12'])

            # 信贷交易信息-资金压力解析-应还贷款总额5个月前
            self.variables["repay_loan_5m_before"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                        report_time_before_5_month)
            #信贷交易信息-资金压力解析-应还本金5个月前
            self.variables["repay_principal_5m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_5_month, 'loan_repay_type', ['13'])
            #信贷交易信息-资金压力解析-应还分期额5个月前
            self.variables["repay_installment_5m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_5_month, 'loan_repay_type', ['11', '12'])

            #信贷交易信息-资金压力解析-应还贷款总额4个月前
            self.variables["repay_loan_4m_before"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                        report_time_before_4_month)
            #信贷交易信息-资金压力解析-应还本金4个月前
            self.variables["repay_principal_4m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_4_month, 'loan_repay_type', ['13'])
            #信贷交易信息-资金压力解析-应还分期额4个月前
            self.variables["repay_installment_4m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_4_month, 'loan_repay_type', ['11', '12'])

            #信贷交易信息-资金压力解析-应还贷款总额3个月前
            self.variables["repay_loan_3m_before"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                        report_time_before_3_month)
            #信贷交易信息-资金压力解析-应还本金3个月前
            self.variables["repay_principal_3m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_3_month, 'loan_repay_type', ['13'])
            #信贷交易信息-资金压力解析-应还分期额3个月前
            self.variables["repay_installment_3m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_3_month, 'loan_repay_type', ['11', '12'])

            #信贷交易信息-资金压力解析-应还贷款总额2个月前
            self.variables["repay_loan_2m_before"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                        report_time_before_2_month)
            #信贷交易信息-资金压力解析-应还本金2个月前
            self.variables["repay_principal_2m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_2_month, 'loan_repay_type', ['13'])
            #信贷交易信息-资金压力解析-应还分期额2个月前
            self.variables["repay_installment_2m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_2_month, 'loan_repay_type', ['11', '12'])

            #信贷交易信息-资金压力解析-应还贷款总额1个月前
            self.variables["repay_loan_1m_before"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                        report_time_before_1_month)
            #信贷交易信息-资金压力解析-应还本金1个月前
            self.variables["repay_principal_1m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_1_month, 'loan_repay_type', ['13'])
            #信贷交易信息-资金压力解析-应还分期额1个月前
            self.variables["repay_installment_1m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_1_month, 'loan_repay_type', ['11', '12'])

            #信贷交易信息-资金压力解析-应还贷款总额1个月后
            self.variables["repay_loan_1m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                        report_time)
            #信贷交易信息-资金压力解析-应还本金1个月后
            self.variables["repay_principal_1m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time, 'loan_repay_type', ['13'])
            #信贷交易信息-资金压力解析-应还分期额1个月后
            self.variables["repay_installment_1m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time, 'loan_repay_type', ['11', '12'])

            #信贷交易信息-资金压力解析-应还贷款总额2个月后
            self.variables["repay_loan_2m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                       report_time_after_1_month)
            #信贷交易信息-资金压力解析-应还本金2个月后
            self.variables["repay_principal_2m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_1_month, 'loan_repay_type', ['13'])
            #信贷交易信息-资金压力解析-应还分期额2个月后
            self.variables["repay_installment_2m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_1_month, 'loan_repay_type', ['11', '12'])

            #信贷交易信息-资金压力解析-应还贷款总额3个月后
            self.variables["repay_loan_3m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                       report_time_after_2_month)
            #信贷交易信息-资金压力解析-应还本金3个月后
            self.variables["repay_principal_3m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_2_month, 'loan_repay_type', ['13'])
            #信贷交易信息-资金压力解析-应还分期额3个月后
            self.variables["repay_installment_3m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_2_month, 'loan_repay_type', ['11', '12'])

            #信贷交易信息-资金压力解析-应还贷款总额4个月后
            self.variables["repay_loan_4m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                       report_time_after_3_month)
            #信贷交易信息-资金压力解析-应还本金4个月后
            self.variables["repay_principal_4m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_3_month, 'loan_repay_type', ['13'])
            #信贷交易信息-资金压力解析-应还分期额4个月后
            self.variables["repay_installment_4m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_3_month, 'loan_repay_type', ['11', '12'])

            # 信贷交易信息-资金压力解析-应还贷款总额5个月后
            self.variables["repay_loan_5m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                       report_time_after_4_month)
            # 信贷交易信息-资金压力解析-应还本金5个月后
            self.variables["repay_principal_5m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_4_month, 'loan_repay_type', ['13'])
            # 信贷交易信息-资金压力解析-应还分期额5个月后
            self.variables["repay_installment_5m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_4_month, 'loan_repay_type', ['11', '12'])

            # 信贷交易信息-资金压力解析-应还贷款总额6个月后
            self.variables["repay_loan_6m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                       report_time_after_5_month)
            # 信贷交易信息-资金压力解析-应还本金6个月后
            self.variables["repay_principal_6m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_5_month, 'loan_repay_type', ['13'])
            # 信贷交易信息-资金压力解析-应还分期额6个月后
            self.variables["repay_installment_6m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_5_month, 'loan_repay_type', ['11', '12'])

            # 信贷交易信息-资金压力解析-应还贷款总额7个月后
            self.variables["repay_loan_7m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                       report_time_after_6_month)
            # 信贷交易信息-资金压力解析-应还本金7个月后
            self.variables["repay_principal_7m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_6_month, 'loan_repay_type', ['13'])
            # 信贷交易信息-资金压力解析-应还分期额7个月后
            self.variables["repay_installment_7m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_6_month, 'loan_repay_type', ['11', '12'])

            # 信贷交易信息-资金压力解析-应还贷款总额8个月后
            self.variables["repay_loan_8m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                       report_time_after_7_month)
            # 信贷交易信息-资金压力解析-应还本金8个月后
            self.variables["repay_principal_8m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_7_month, 'loan_repay_type', ['13'])
            # 信贷交易信息-资金压力解析-应还分期额8个月后
            self.variables["repay_installment_8m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_7_month, 'loan_repay_type', ['11', '12'])

            # 信贷交易信息-资金压力解析-应还贷款总额9个月后
            self.variables["repay_loan_9m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                       report_time_after_8_month)
            # 信贷交易信息-资金压力解析-应还本金9个月后
            self.variables["repay_principal_9m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_8_month, 'loan_repay_type', ['13'])
            # 信贷交易信息-资金压力解析-应还分期额9个月后
            self.variables["repay_installment_9m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_8_month, 'loan_repay_type', ['11', '12'])

            # 信贷交易信息-资金压力解析-应还贷款总额10个月后
            self.variables["repay_loan_10m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                       report_time_after_9_month)
            # 信贷交易信息-资金压力解析-应还本金10个月后
            self.variables["repay_principal_10m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_9_month, 'loan_repay_type', ['13'])
            # 信贷交易信息-资金压力解析-应还分期额10个月后
            self.variables["repay_installment_10m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_9_month, 'loan_repay_type', ['11', '12'])

            # 信贷交易信息-资金压力解析-应还贷款总额11个月后
            self.variables["repay_loan_11m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                        report_time_after_10_month)
            # 信贷交易信息-资金压力解析-应还本金10个月后
            self.variables["repay_principal_11m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_10_month, 'loan_repay_type', ['13'])
            # 信贷交易信息-资金压力解析-应还分期额10个月后
            self.variables["repay_installment_11m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_10_month, 'loan_repay_type', ['11', '12'])

            # 信贷交易信息-资金压力解析-应还贷款总额12个月后
            self.variables["repay_loan_12m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                        report_time_after_11_month)
            # 信贷交易信息-资金压力解析-应还本金12个月后
            self.variables["repay_principal_12m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_11_month, 'loan_repay_type', ['13'])
            # 信贷交易信息-资金压力解析-应还分期额12个月后
            self.variables["repay_installment_12m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_11_month, 'loan_repay_type', ['11', '12'])

        undestory_avg_use=pcredit_info_df.loc[:,'undestory_avg_use'].sum()
        undestory_semi_avg_overdraft=pcredit_info_df.loc[:,'undestory_semi_avg_overdraft'].sum()
        repay_credit_n_month=undestory_avg_use+undestory_semi_avg_overdraft
        # 信贷交易信息-资金压力解析-应还贷记卡6个月前
        self.variables["repay_credit_6m_before"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡5个月前
        self.variables["repay_credit_5m_before"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡4个月前
        self.variables["repay_credit_4m_before"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡3个月前
        self.variables["repay_credit_3m_before"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡2个月前
        self.variables["repay_credit_2m_before"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡1个月前
        self.variables["repay_credit_1m_before"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡1个月后
        self.variables["repay_credit_1m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡2个月后
        self.variables["repay_credit_2m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡3个月后
        self.variables["repay_credit_3m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡4个月后
        self.variables["repay_credit_4m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡5个月后
        self.variables["repay_credit_5m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡6个月后
        self.variables["repay_credit_6m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡7个月后
        self.variables["repay_credit_7m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡8个月后
        self.variables["repay_credit_8m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡9个月后
        self.variables["repay_credit_9m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡10个月后
        self.variables["repay_credit_10m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡11个月后
        self.variables["repay_credit_11m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡12个月后
        self.variables["repay_credit_12m_after"] = repay_credit_n_month


        # 信贷交易信息-贷款信息-近五年经营性贷款余额变化-时间节点
        busi_loan_date_list = [report_time_before_4_year, report_time_before_3_year, report_time_before_2_year,
                               report_time_before_1_year, report_time_before_0_year]
        self.variables["busi_loan_date"] = busi_loan_date_list
        # 信贷交易信息-贷款信息-近五年经营性贷款余额变化-贷款余额
        pcredit_loan_type_df=pcredit_loan_df[(pcredit_loan_df['account_type'].isin(['01','02','03'])) and
                                             ((pcredit_loan_df['loan_type'].isin(['01','07','99'])) or ((pcredit_loan_df['loan_type']=='04') and (pcredit_loan_df['loan_amount'])))]
        busi_loan_balance_list=[]
        if not pcredit_loan_type_df.empty:
            pcredit_loan_type_temp_df = pd.merge(pcredit_loan_type_df,pcredit_acc_speculate_df,left_on='id',right_on='record_id')
            loan_balance_4_year_before = self.util_get_repay_n_month_before_loan_balance(pcredit_loan_type_temp_df,
                                                                       report_time_before_4_year)
            loan_balance_3_year_before = self.util_get_repay_n_month_before_loan_balance(pcredit_loan_type_temp_df,
                                                                                         report_time_before_3_year)
            loan_balance_2_year_before = self.util_get_repay_n_month_before_loan_balance(pcredit_loan_type_temp_df,
                                                                                         report_time_before_2_year)
            loan_balance_1_year_before = self.util_get_repay_n_month_before_loan_balance(pcredit_loan_type_temp_df,
                                                                                         report_time_before_1_year)
            loan_balance_0_year_before = self.util_get_repay_n_month_before_loan_balance(pcredit_loan_type_temp_df,
                                                                                         report_time_before_0_year)
            busi_loan_balance_list.append(loan_balance_4_year_before)
            busi_loan_balance_list.append(loan_balance_3_year_before)
            busi_loan_balance_list.append(loan_balance_2_year_before)
            busi_loan_balance_list.append(loan_balance_1_year_before)
            busi_loan_balance_list.append(loan_balance_0_year_before)
        else:
            busi_loan_balance_list=[0,0,0,0,0]
        self.variables["busi_loan_balance"]=busi_loan_balance_list






    def util_get_repay_n_month_before(self,df,date,param=None,param_value_list=None):
        year=date.year
        month=date.month
        df=df[(df['year']==year) and (df['month']==month)]
        if param is not None:
            df=df[df[param].isin(param_value_list)]
        return df.loc[:,'repay_amount'].sum()

    def util_get_repay_n_month_before_loan_balance(self,df,date,param,param_value_list):
        year=date.year
        month=date.month
        df=df[(df['year']==year) and (df['month']==month)]
        if param is not None:
            df=df[df[param].isin(param_value_list)]
        return df.loc[:,'loan_balance'].sum()



    def acc_speculate_df_time_format(self, df):
        df['repay_month'] = pd.to_datetime(df['repay_month'],format="%Y-%m")
        df['year'] = df['repay_month'].apply(lambda x: x.year)
        df['month'] = df['repay_month'].apply(lambda x: x.month)
        return df




