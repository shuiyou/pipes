# @Time : 2020/4/23 7:06 PM 
# @Author : lixiaobo
# @File : basic_info_handle.py.py 
# @Software: PyCharm
from mapping.module_processor import ModuleProcessor


# 基本信息处理


class BasicInfoProcessor(ModuleProcessor):

    def process(self):
        print("BasicInfoProcessor process")
        self.variables["report_id"] = self.cached_data["report_id"]

        self._rhzx_business_loan_overdue_cnt()
        self._public_sum_count()
        self._credit_fiveLevel_a_level_cnt()
        self._loan_fiveLevel_a_level_cnt()
        self._business_loan_average_overdue_cnt()

    # 经营性贷款逾期笔数
    def _rhzx_business_loan_overdue_cnt(self):
        loan_df = self.cached_data.get("pcredit_loan")
        repayment_df = self.cached_data.get("pcredit_repayment")
        if loan_df is None or loan_df.empty or repayment_df is None or repayment_df.empty:
            return

        loan_df = loan_df.query('account_type in ["01", "02", "03"] and ((loan_type in '
                                '["01", "07", "99"]) or (loan_type == "04") and principal_amount > 200000)')

        if loan_df.empty:
            return

        repayment_df = repayment_df.query('record_id in ' + str(list(loan_df.id)) + ' and status == 1')
        self.variables["rhzx_business_loan_overdue_cnt"] = len(repayment_df)

    # 呆账、资产处置、保证人代偿笔数
    def _public_sum_count(self):
        default_info_df = self.cached_data.get("pcredit_default_info")
        if default_info_df is None or default_info_df.empty:
            return

        df = default_info_df.query('(default_type == "01" and default_subtype == "0103") or default_type == "02"')
        if df is not None:
            self.variables["public_sum_count"] = df.shape[0]

    # 贷记卡五级分类存在“可疑、损失”
    def _credit_fiveLevel_a_level_cnt(self):
        df = self.cached_data.get("pcredit_loan")
        if df is None or df.empty:
            return

        df = df.query('account_type in ["04", "05"] and latest_category in ["4", "5"]')
        if df is not None:
            self.variables["credit_fiveLevel_a_level_cnt"] = df.shape[0]

    # 贷款五级分类存在“次级、可疑、损失”
    def _loan_fiveLevel_a_level_cnt(self):
        df = self.cached_data.get("pcredit_loan")
        if df is None or df.empty:
            return

        df = df.query('account_type in ["01", "02", "03"] and category in ["3", "4", "5"]')
        if df is not None:
            self.variables["credit_fiveLevel_a_level_cnt"] = df.shape[0]

    # 还款方式为等额本息分期偿还的经营性贷款最大连续逾期期数
    def _business_loan_average_overdue_cnt(self):
        '''
        1.从pcredit_loan中选择所有report_id=report_id且account_type=01,02,03且(loan_type=01,07,99或者(loan_type=04且principal_amount>200000))的id,
        2.对于每一个id,count(pcredit_payment中所有record_id=id且loan_repay_type包含"等额本息"且status是数字的记录)如果count>0则变量+1
        '''
        credit_loan_df = self.cached_data.get("pcredit_loan")
        repayment_df = self.cached_data.get("pcredit_repayment")

        if credit_loan_df is None or credit_loan_df.empty or repayment_df is None or repayment_df.empty:
            return

        credit_loan_df = credit_loan_df.query('account_type in ["01", "02", "03"] and (loan_type in ["01", "07", '
                                              '"99"] or (loan_type == "04" and principal_amount>200000)) and '
                                              'loan_repay_type.str.contains("等额本息")')

        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        if repayment_df is not None:
            count = 0
            for index, row in repayment_df.iterrows():
                if row["status"] and row["status"].isdigit():
                    count = count + 1
            self.variables["business_loan_average_overdue_cnt"] = count
