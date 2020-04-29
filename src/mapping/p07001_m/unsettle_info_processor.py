from mapping.module_processor import ModuleProcessor


# unsettled开头的变量


class UnSettleInfoProcessor(ModuleProcessor):
    def process(self):
        self._unsettled_busLoan_agency_number()
        self._unsettled_consume_agency_cnt()

    # 有经营性贷款在贷余额的合作机构数
    def _unsettled_busLoan_agency_number(self):
        # 1.从pcredit_loan中选择所有report_id=report_id且account_type=01,02,03且(loan_type=01,07,99或者(account_type=04且principal_amount>200000))的account_org
        # 2.统计1中不同的account_org数目
        credit_loan_df = self.cached_data["pcredit_loan"]
        credit_loan_df = credit_loan_df.query('account_type in["01", "02", "03"] and '
                                              '(loan_type in ["01", "07", "99"] '
                                              'or (account_type == "04" and principal_amount>200000))')

        if credit_loan_df.empty:
            return
        count = credit_loan_df.dropna(subset=["account_org"])["account_org"].unique().size
        self.variables["unsettled_busLoan_agency_number"] = count

    # 未结清消费性贷款机构数
    def _unsettled_consume_agency_cnt(self):
        # 1.从pcredit_loan中选择所有report_id=report_id且account_type=01,02,03且loan_type=04且principal_amount<=200000且loan_balance>0的account_org
        # 2.统计1中不同的account_org数目
        credit_loan_df = self.cached_data["pcredit_loan"]
        credit_loan_df = credit_loan_df.query('account_type in["01", "02", "03"] and loan_type == "04" '
                                              'and principal_amount > 200000 and loan_balance > 0')

        if credit_loan_df.empty:
            return
        count = credit_loan_df.dropna(subset=["account_org"])["account_org"].unique().size
        self.variables["unsettled_consume_agency_cnt"] = count
