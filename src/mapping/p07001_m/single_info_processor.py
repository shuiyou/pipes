# @Time : 2020/4/28 3:08 PM 
# @Author : lixiaobo
# @File : single_info_processor.py.py 
# @Software: PyCharm
from mapping.module_processor import ModuleProcessor

# single开头的相关的变量
from product.date_time_util import after_ref_date


class SingleInfoProcessor(ModuleProcessor):
    def process(self):
        self._single_house_overdue_2year_cnt()
        self._single_car_overdue_2year_cnt()
        self._single_consume_overdue_2year_cnt()
        self._single_credit_overdue_cnt_2y()
        self._single_house_loan_overdue_cnt_2y()
        self._single_car_loan_overdue_cnt_2y()
        self._single_consume_loan_overdue_cnt_2y()

    # 单笔房贷近2年内最大逾期次数
    def _single_house_overdue_2year_cnt(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且loan_type=03,05,06的id
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time两年内的记录)
        # 3.从2中所有结果中选取最大值
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")

        if credit_loan_df is None or credit_loan_df.empty or repayment_df is None or repayment_df.empty:
            return

        credit_loan_df = credit_loan_df.query('account_type in ["01", "02", "03"] and loan_type in ["03", "05", "06"]')
        if credit_loan_df.empty:
            return

        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        if repayment_df is not None and not repayment_df.empty:
            report_time = self.cached_data["report_time"]
            status_list = []
            for index, row in repayment_df.iterrows():
                if row["status"] and row["status"].isdigit() or (row['repayment_amt'] and row['repayment_amt'] > 0):
                    if after_ref_date(row.jhi_year, row.month, report_time.year - 2, report_time.month):
                        status_list.append(int(row["status"]))
            self.variables["single_house_overdue_2year_cnt"] = 0 if len(status_list) == 0 else max(status_list)

    # 单笔车贷近2年内最大逾期次数
    def _single_car_overdue_2year_cnt(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且loan_type=02的id
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time两年内的记录)
        # 3.从2中所有结果中选取最大值
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")

        if credit_loan_df is None or credit_loan_df.empty or repayment_df is None or repayment_df.empty:
            return

        credit_loan_df = credit_loan_df.query('account_type in ["01", "02", "03"] and loan_type == "02"')
        if credit_loan_df.empty:
            return

        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        if repayment_df is not None and not repayment_df.empty:
            report_time = self.cached_data["report_time"]
            status_list = []
            for index, row in repayment_df.iterrows():
                if row["status"] and row["status"].isdigit() or (row['repayment_amt'] and row['repayment_amt'] > 0):
                    if after_ref_date(row.jhi_year, row.month, report_time.year - 2, report_time.month):
                        status_list.append(int(row["status"]))
            self.variables["single_car_overdue_2year_cnt"] = 0 if len(status_list) == 0 else max(status_list)

    # 单笔消费性贷款近2年内最大逾期次数
    def _single_consume_overdue_2year_cnt(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且loan_type=04且loan_amount<=200000的id
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time两年内的记录)
        # 3.从2中所有结果中选取最大值
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")

        if credit_loan_df is None or credit_loan_df.empty or repayment_df is None or repayment_df.empty:
            return

        credit_loan_df = credit_loan_df.query('account_type in ["01", "02", "03"] and loan_type == "04" '
                                              'and loan_amount <= 200000')
        if credit_loan_df.empty:
            return

        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        count = 0
        if repayment_df is not None and not repayment_df.empty:
            report_time = self.cached_data["report_time"]
            for index, row in repayment_df.iterrows():
                if row["status"] and row["status"].isdigit() or (row['repayment_amt'] and row['repayment_amt'] > 0):
                    if after_ref_date(row.jhi_year, row.month, report_time.year - 2, report_time.month):
                        count = count + 1
        self.variables["single_consume_overdue_2year_cnt"] = count

    # 单张贷记卡2年内逾期次数
    def _single_credit_overdue_cnt_2y(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04,05的id;
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time两年内的记录);
        # 3.从2中所有结果中选取最大值
        self._max_overdue_cacl(account_type=["04", "05"], var_name="single_credit_overdue_cnt_2y")

    # 单笔房贷2年内逾期次数
    def _single_house_loan_overdue_cnt_2y(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且loan_type=03,05,06的id;
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time两年内的记录);
        # 3.从2中所有结果中选取最大值
        self._max_overdue_cacl(account_type=["01", "02", "03"], loan_type=["03", "05", "06"],
                               var_name="single_house_loan_overdue_cnt_2y")

    # 单笔车贷2年内逾期次数
    def _single_car_loan_overdue_cnt_2y(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且loan_type=02的id;
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time两年内的记录);
        # 3.从2中所有结果中选取最大值
        self._max_overdue_cacl(account_type=["01", "02", "03"], loan_type=["02"],
                               var_name="single_car_loan_overdue_cnt_2y")

    # 单笔消费贷2年内逾期次数
    def _single_consume_loan_overdue_cnt_2y(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且loan_type=04的id;
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time两年内的记录);
        # 3.从2中所有结果中选取最大值
        self._max_overdue_cacl(account_type=["01", "02", "03"], loan_type=["04"],
                               var_name="single_consume_loan_overdue_cnt_2y")

    def _max_overdue_cacl(self, account_type=None, loan_type=None, var_name=None, within_year=2):
        if not account_type and not loan_type:
            return

        loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")

        if loan_df.empty or repayment_df.empty:
            return

        if account_type and loan_type:
            loan_df = loan_df.query('account_type in ' + str(account_type) + 'and loan_type in ' + str(loan_type))
        elif account_type:
            loan_df = loan_df.query('account_type in ' + str(account_type))
        elif loan_type:
            loan_df = loan_df.query('loan_type in ' + str(loan_type))

        if loan_df.empty:
            return

        repayment_df = repayment_df.query('record_id in ' + str(list(loan_df.id)) + ' and (repayment_amt > 0 or status.str.isdigit())')
        self.variables[var_name] = repayment_df.groupby(by='record_id').size().max()
        # if not repayment_df.empty:
        #     report_time = self.cached_data["report_time"]
        #     status_list = []
        #     for index, row in repayment_df.iterrows():
        #         if row["status"] and row["status"].isdigit():
        #             if after_ref_date(row.jhi_year, row.month, report_time.year - within_year, report_time.month):
        #                 status_list.append(int(row["status"]))
        #     self.variables[var_name] = 0 if len(status_list) == 0 else max(status_list)
