# @Time : 2020/4/30 8:54 AM
# @Author : lixiaobo
# @File : total_info_processor.py.py
# @Software: PyCharm
import pandas as pd

from mapping.module_processor import ModuleProcessor


# total开头的相关信息
from mapping.p07001_m.calculator import split_by_duration_seq
from product.date_time_util import after_ref_date


class TotalInfoProcessor(ModuleProcessor):
    def process(self):
        self._total_consume_loan_overdue_cnt_5y()
        self._total_consume_loan_overdue_money_5y()
        self._total_bank_credit_limit()
        self._total_bank_loan_balance()

    # 消费贷5年内总逾期次数
    def _total_consume_loan_overdue_cnt_5y(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且(loan_type=02,03,05,06或者(loan_type=04且loan_amount<=200000))的id
        # 2.对每一个id,count(pcredit_payment中record_id=id且repayment_amt>0且还款时间在report_time五年内的记录)
        # 3.将2中所有结果加总
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")
        credit_loan_df = credit_loan_df.query('account_type in ["01", "02", "03"] '
                                              'and (loan_type in ["02", "03", "05", "06"]'
                                              ' or (loan_type == "04" and loan_amount <= 200000))')
        if credit_loan_df.empty:
            return

        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        if repayment_df is not None:
            count = 0
            report_time = self.cached_data["report_time"]
            for index, row in repayment_df.iterrows():
                if (pd.notna(row["repayment_amt"]) and row["repayment_amt"] > 0) \
                        or (pd.notna(row["status"]) and row["status"].isdigit()):
                    if after_ref_date(row.jhi_year, row.month, report_time.year - 5, report_time.month):
                        count = count + 1
            self.variables["total_consume_loan_overdue_cnt_5y"] = count

    # 消费贷5年内总逾期金额
    def _total_consume_loan_overdue_money_5y(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且loan_type=02,03,04,05,06的id;
        # 2.对每一个id,从pcredit_payment中选取所有record_id=id且status是数字的记录,将每段连续逾期的最后一笔repayment_amt加总;
        # 3.将2中所有结果加总
        credit_loan = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data["pcredit_repayment"]
        account_type = ["01", "02", "03"]
        loan_type = ["02", "03", "04", "05", "06"]
        credit_loan = credit_loan.query('account_type in ' + str(account_type) + 'and loan_type in ' + str(loan_type))

        if credit_loan.empty or repayment_df.empty:
            return

        val_lists = []
        for loan in credit_loan.itertuples():
            df = repayment_df.query('record_id == ' + str(loan.id))
            df = df.sort_values("id")
            split_by_duration_seq(df, val_lists)
        final_result = sum(map(lambda x: 0 if pd.isna(x[-1]) else x[-1], val_lists))
        self.variables["total_consume_loan_overdue_money_5y"] = final_result

    # 银行授信总额
    def _total_bank_credit_limit(self):
        # 1.从pcredit_info中选取所有report_id=report_id的所有nonRevolloan_totalcredit,revolcredit_totalcredit,revolloan_totalcredit,undestroy_limit,undestory_semi_limit;
        # 2.将1中所有字段值相加
        credit_info_df = self.cached_data["pcredit_info"]
        df = credit_info_df.loc[:,
             ["non_revolloan_totalcredit", "revolcredit_totalcredit", "revolloan_totalcredit", "undestroy_limit",
              "undestory_semi_limit"]]
        if df.empty:
            return
        value = df.sum().sum()
        value = value if pd.notna(value) else 0
        self.variables["total_bank_credit_limit"] = value

    # 银行总余额
    def _total_bank_loan_balance(self):
        # 1.从pcredit_info中选取所有report_id=report_id的所有nonRevolloan_balance,revolcredit_balance,revolloan_balance,undestroy_used_limit,undestory_semi_overdraft;
        # 2.将1中所有字段值相加
        credit_info_df = self.cached_data["pcredit_info"]
        df = credit_info_df.loc[:, ["non_revolloan_balance", "revolcredit_balance", "revolloan_balance", "undestroy_used_limit", "undestory_semi_overdraft"]]
        if df.empty:
            return
        value = df.sum().sum()
        value = value if pd.notna(value) else 0
        self.variables["total_bank_loan_balance"] = value
