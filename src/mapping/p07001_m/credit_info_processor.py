# @Time : 2020/4/28 2:52 PM 
# @Author : lixiaobo
# @File : credit_info_processor.py.py 
# @Software: PyCharm

from mapping.module_processor import ModuleProcessor

# credit开头的相关变量
from product.date_time_util import after_ref_date


class CreditInfoProcessor(ModuleProcessor):
    def process(self):
        self._credit_fiveLevel_a_level_cnt()
        self._credit_now_overdue_money()
        self._credit_overdue_max_month()
        self._credit_overdrawn_2card()
        self._credit_overdue_5year()
        self._credit_max_overdue_2year()
        self._credit_fiveLevel_b_level_cnt()

    # 贷记卡五级分类存在“可疑、损失”
    def _credit_fiveLevel_a_level_cnt(self):
        df = self.cached_data.get("pcredit_loan")
        if df is None or df.empty:
            return

        df = df.query('account_type in ["04", "05"] and latest_category in ["4", "5"]')
        if df is not None:
            self.variables["credit_fiveLevel_a_level_cnt"] = df.shape[0]

    # 贷记卡当前逾期金额
    def _credit_now_overdue_money(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04的overdue_amount相加
        # 2.从pcredit_loan中选取所有report_id=report_id且account_type=05的id,对每一个id,在pcredit_repayment中record_id=id记录中找到最近一笔还款记录中的repayment_amt,将所有repayment_amt加总
        # 3.变量值=1和2中结果相加
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")
        overdue_amt = credit_loan_df.query('account_type == "04"').fillna(0)["overdue_amount"].sum()

        loan_ids = credit_loan_df.query('account_type == "05"')["id"]
        for loan_id in loan_ids:
            df = repayment_df.query('record_id == ' + str(loan_id))
            df = df.dropna(subset=["repayment_amt"])
            if not df.empty:
                overdue_amt = overdue_amt + df.iloc[0].repayment_amt

        self.variables["credit_now_overdue_money"] = overdue_amt

    # 贷记卡最大连续逾期月份数
    def _credit_overdue_max_month(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04,05的id
        # 2.对每一个id,max(pcredit_payment中record_id=id且status是数字的status)
        # 3.从2中所有结果中选取最大值"
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")

        if credit_loan_df is None or credit_loan_df.empty or repayment_df is None or repayment_df.empty:
            return

        credit_loan_df = credit_loan_df.query('account_type in ["04", "05"]')
        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        if not repayment_df.empty:
            status_list = []
            for index, row in repayment_df.iterrows():
                if row["status"] and row["status"].isdigit():
                    status_list.append(int(row["status"]))
            self.variables["credit_overdue_max_month"] = 0 if len(status_list) == 0 else max(status_list)

    # 贷记卡总透支率达80%且存在2张贷记卡最低额还款
    def _credit_overdrawn_2card(self):
        # 1.从pcredit_loan中选取所有report_id=report_id的undestroy_limit,undestory_used_limit,undestory_semi_overdraft,undestory_avg_use,undestory_semi_avg_overdraft,undestory_semi_limit,计算max(undestory_used_limit+undestory_semi_overdraft,undestory_avg_use+undestory_semi_avg_overdraft)/(undestroy_limit+undestory_semi_limit)
        # 2.从pcredit_loan中选取所有report_id=report_id且account_type=04,05的记录,统计其中满足条件repay_amount*2>amount_replay_amount的记录
        # 3.若1中结果>=0.8且2中结果>=2,则变量=1,否则=0"
        pass

    # 总计贷记卡5年内逾期次数
    def _credit_overdue_5year(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04,05的id
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time五年内的记录)
        # 3.将2中所有结果加总"

        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")

        if credit_loan_df is None or credit_loan_df.empty or repayment_df is None or repayment_df.empty:
            return

        credit_loan_df = credit_loan_df.query('account_type in ["04", "05"]')

        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        report_time = self.cached_data["report_time"]
        if repayment_df is not None:
            status_list = []
            for index, row in repayment_df.iterrows():
                if row["status"] and row["status"].isdigit():
                    if after_ref_date(row.jhi_year, row.month, report_time.year - 5, report_time.month):
                        status_list.append(int(row["status"]))
            self.variables["credit_overdue_5year"] = 0 if len(status_list) == 0 else max(status_list)

    # 单张贷记卡近2年内最大逾期次数
    def _credit_max_overdue_2year(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04,05的id
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time两年内的记录)
        # 3.从2中所有结果中选取最大值
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data["pcredit_repayment"]

        credit_loan_df = credit_loan_df.query('account_type in ["04", "05"]')
        if credit_loan_df.empty or repayment_df.empty:
            return

        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        report_time = self.cached_data["report_time"]
        if repayment_df is not None:
            status_list = []
            for index, row in repayment_df.iterrows():
                if row["status"] and row["status"].isdigit():
                    if after_ref_date(row.jhi_year, row.month, report_time.year - 2, report_time.month):
                        status_list.append(int(row["status"]))
            self.variables["credit_max_overdue_2year"] = 0 if len(status_list) == 0 else max(status_list)

    # 贷记卡五级分类存在“次级
    def _credit_fiveLevel_b_level_cnt(self):
        # count(pcredit_loan中所有report_id=report_id且account_type=04,05且latest_category=3的记录)
        credit_loan_df = self.cached_data["pcredit_loan"]
        credit_loan_df = credit_loan_df.query('account_type == "04" and latest_category == "3"')
        count = credit_loan_df.shape[0]

        self.variables["credit_fiveLevel_b_level_cnt"] = count
