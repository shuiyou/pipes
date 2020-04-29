# @Time : 2020/4/28 2:54 PM 
# @Author : lixiaobo
# @File : loan_info_processor.py 
# @Software: PyCharm
from mapping.module_processor import ModuleProcessor

# loan开头相关的变量
from product.date_time_util import after_ref_date


class LoanInfoProcessor(ModuleProcessor):
    def process(self):
        self._loan_fiveLevel_a_level_cnt()
        self._loan_now_overdue_money()
        self._loan_credit_query_3month_cnt()
        self._loan_consume_overdue_5year()
        self._loan_credit_small_loan_query_3month_cnt()
        self._loan_fiveLevel_b_level_cnt()
        self._loan_scured_five_a_level_abnormality_cnt()
        self._loan_scured_five_b_level_abnormality_cnt()

    # 贷款五级分类存在“次级、可疑、损失”
    def _loan_fiveLevel_a_level_cnt(self):
        df = self.cached_data.get("pcredit_loan")
        if df is None or df.empty:
            return

        df = df.query('account_type in ["01", "02", "03"] and category in ["3", "4", "5"]')
        if df is not None:
            self.variables["loan_fiveLevel_a_level_cnt"] = df.shape[0]

    # 贷款当前逾期金额
    def _loan_now_overdue_money(self):
        # 从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03的overdue_amount加总
        credit_loan_df = self.cached_data["pcredit_loan"]
        amt = credit_loan_df.query('account_type in["01", "02"]').dropna(subset=["overdue_amount"])["overdue_amount"].sum()
        self.variables["loan_now_overdue_money"] = amt

    # 近三个月征信查询（贷款审批及贷记卡审批）次数
    def _loan_credit_query_3month_cnt(self):
        # count(pcredit_query_record中report_id=report_id且记录时间在report_time三个月内且=01,02的记录)
        query_record_df = self.cached_data["pcredit_query_record"]
        print("query_record_df\n", query_record_df)

        report_time = self.cached_data["report_time"]
        df = query_record_df.query('reason in["01", "02"]').dropna(subset=["jhi_time"])
        count = 0
        if not df.empty:
            for index, row in df.iterrows():
                if after_ref_date(row.jhi_year, row.month, report_time.year - 3, report_time.month):
                    count = count + 1
        self.variables["loan_credit_query_3month_cnt"] = count

    # 总计消费性贷款（含车贷、房贷、其他消费性贷款）5年内逾期次数
    def _loan_consume_overdue_5year(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且(loan_type=02,03,05,06或者(loan_type=04且principal_amount<=200000))的id
        # 2.对每一个id,count(pcredit_payment中record_id=id且repayment_amt>0且还款时间在report_time五年内的记录)
        # 3.将2中所有结果加总
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")
        credit_loan_df = credit_loan_df.query('account_type in ["01", "02", "03"] '
                                              'and (loan_type in ["02", "03", "05", "06"]'
                                              ' or (loan_type == "04" and principal_amount <= 200000))')

        if credit_loan_df.empty:
            return

        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        if repayment_df is not None:
            count = 0
            report_time = self.cached_data["report_time"]
            for index, row in repayment_df.iterrows():
                if row["repayment_amt"] and row["repayment_amt"].isdigit():
                    if after_ref_date(row.jhi_year, row.month, report_time.year - 5, report_time.month):
                        count = count + 1
            self.variables["loan_consume_overdue_5year"] = count

    # 近三个月小额贷款公司贷款审批查询次数
    def _loan_credit_small_loan_query_3month_cnt(self):
        # count(pcredit_query_record中report_id=report_id且记录时间在report_time三个月内且reason=01且operator包含"小额贷款机构"的记录)
        query_record_df = self.cached_data["pcredit_query_record"]
        if not query_record_df.empty:
            query_record_df = query_record_df[query_record_df["operator"].str.contains("小额贷款机构")]
            query_record_df = query_record_df.query('reason == "01"')
            report_time = self.cached_data["report_time"]
            count = 0
            for index, row in query_record_df.iterrows():
                if after_ref_date(row.jhi_year, row.month, report_time.year, report_time.month - 3):
                    count = count + 1
            self.variables["loan_credit_small_loan_query_3month_cnt"] = count

    # 贷款五级分类存在"关注"
    def _loan_fiveLevel_b_level_cnt(self):
        # count(pcredit_loan中所有report_id=report_id且account_type=01,02,03且category=2的记录)
        credit_loan_df = self.cached_data["pcredit_loan"]
        if credit_loan_df.empty:
            return

        df = credit_loan_df.query('account_type in ["01", "02", "03"] and category == "2"')
        self.variables["loan_fiveLevel_b_level_cnt"] = df.shape[0]

    # 对外担保五级分类存在“次级、可疑、损失”
    def _loan_scured_five_a_level_abnormality_cnt(self):
        # count(pcredit_loan中所有report_id=report_id且account_type=06且category=3,4,5的记录)
        credit_loan_df = self.cached_data["pcredit_loan"]
        if credit_loan_df.empty:
            return

        df = credit_loan_df.query('account_type == "06" and category in["3", "4", "5"]')
        self.variables["loan_scured_five_a_level_abnormality_cnt"] = df.shape[0]

    # 对外担保五级分类存在"关注"
    def _loan_scured_five_b_level_abnormality_cnt(self):
        # count(pcredit_loan中所有report_id=report_id且account_type=06且category=2的记录)
        df = self.cached_data["pcredit_loan"]
        df = df.query('account_type == "06" and category == "2"')
        self.variables["loan_scured_five_b_level_abnormality_cnt"] = df.shape[0]








