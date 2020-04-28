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
