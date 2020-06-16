# @Time : 2020/4/28 2:52 PM 
# @Author : lixiaobo
# @File : credit_info_processor.py.py 
# @Software: PyCharm
import pandas as pd

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
        self._credit_financial_tension()
        self._credit_activated_number()
        self._credit_min_payed_number()
        self._credit_fiveLevel_c_level_cnt()
        self._credit_now_overdue_cnt()
        self._credit_total_overdue_cnt()
        self._credit_status_bad_cnt()  # 贷记卡账户状态存在"呆账"
        self._credit_status_legal_cnt()  # 贷记卡账户状态存在"司法追偿"
        self._credit_status_b_level_cnt()  # 贷记卡账户状态存在"银行止付、冻结"

    # 贷记卡五级分类存在“可疑、损失”
    def _credit_fiveLevel_a_level_cnt(self):
        df = self.cached_data.get("pcredit_loan")
        if df is None or df.empty:
            return

        df = df.query('account_type in ["04", "05"] and category in ["4", "5"]')
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
                df = df.sort_values(by=['jhi_year', 'month'], ascending=False)
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
        # 1.从pcredit_info中选取所有report_id=report_id的undestroy_limit,undestory_used_limit,undestory_semi_overdraft,undestory_avg_use,undestory_semi_avg_overdraft,undestory_semi_limit,计算max(undestory_used_limit+undestory_semi_overdraft,undestory_avg_use+undestory_semi_avg_overdraft)/(undestroy_limit+undestory_semi_limit)
        # 2.从pcredit_loan中选取所有report_id=report_id且account_type=04,05的记录,统计其中满足条件repay_amount*2>amount_replay_amount的记录
        # 3.若1中结果>=0.8且2中结果>=2,则变量=1,否则=0"
        credit_info_df = self.cached_data["pcredit_info"]
        credit_loan_df = self.cached_data["pcredit_loan"]
        df = credit_info_df.fillna(0)
        v1_satisfy = False
        for row in df.itertuples():
            v1 = max(row.undestory_used_limit + row.undestory_semi_overdraft, row.undestory_avg_use + row.undestory_semi_avg_overdraft)
            v2 = row.undestroy_limit + row.undestory_semi_limit
            if v2 > 0:
                v1_satisfy |= (v1/v2) >= 0.8
            self.variables['total_credit_used_rate'] = v1 / v2

        df = credit_loan_df.query('account_type in ["04", "05"]')
        df = df.fillna(0)
        v2_count = 0
        for row in df.itertuples():
            if row.repay_amount*2 > row.amout_replay_amount:
                v2_count = v2_count + 1
        self.variables['total_credit_min_repay_cnt'] = v2_count

        final_result = v1_satisfy and v2_count >= 2
        self.variables["credit_overdrawn_2card"] = 1 if final_result else 0

    # 总计贷记卡5年内逾期次数
    def _credit_overdue_5year(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04,05的id
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time五年内的记录)
        # 3.将2中所有结果加总"
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")

        if credit_loan_df.empty or repayment_df.empty:
            return

        credit_loan_df = credit_loan_df.query('account_type in ["04", "05"]')

        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        report_time = self.cached_data["report_time"]
        if not repayment_df.empty:
            count = 0
            for index, row in repayment_df.iterrows():
                if pd.notna(row["status"]) and row["status"].isdigit():
                    if after_ref_date(row.jhi_year, row.month, report_time.year - 5, report_time.month):
                        count = count + 1
            self.variables["credit_overdue_5year"] = count

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

        report_time = self.cached_data["report_time"]
        status_list = [0]
        for row in credit_loan_df.itertuples():
            df = repayment_df.query('record_id == ' + str(row.id))
            if df.empty:
                continue
            count = 0
            for index, row in df.iterrows():
                if row["status"] and row["status"].isdigit():
                    if after_ref_date(row.jhi_year, row.month, report_time.year - 2, report_time.month):
                        count = count + 1
            status_list.append(count)
            self.variables["credit_max_overdue_2year"] = max(status_list)

    # 贷记卡五级分类存在“次级
    def _credit_fiveLevel_b_level_cnt(self):
        # count(pcredit_loan中所有report_id=report_id且account_type=04,05且latest_category=3的记录)
        credit_loan_df = self.cached_data["pcredit_loan"]
        credit_loan_df = credit_loan_df.query('account_type == "04" and category == "3"')

        self.variables["credit_fiveLevel_b_level_cnt"] = credit_loan_df.shape[0]

    # 贷记卡资金紧张程度
    def _credit_financial_tension(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04,05的记录
        # 2.计算max(sum(quota_used),sum(avg_overdraft_balance_6))/sum(loan_amount)
        # 3.统计满足条件repay_amount*2>amount_replay_amount的记录
        # 4.计算(3中结果+1)*min(2,2中结果)
        credit_info_df = self.cached_data["pcredit_info"]
        if credit_info_df.empty:
            return
        credit_loan_df = self.cached_data["pcredit_loan"]
        df = credit_loan_df.query('account_type in ["04", "05"]')
        if df.empty:
            return
        undestory_used_limit = self._check_is_null(credit_info_df.loc[0, 'undestory_used_limit'])
        undestory_semi_overdraft = self._check_is_null(credit_info_df.loc[0, 'undestory_semi_overdraft'])
        undestory_avg_use = self._check_is_null(credit_info_df.loc[0, 'undestory_avg_use'])
        undestory_semi_avg_overdraft = self._check_is_null(credit_info_df.loc[0, 'undestory_semi_avg_overdraft'])
        undestroy_limit = self._check_is_null(credit_info_df.loc[0, 'undestroy_limit'])
        undestory_semi_limit = self._check_is_null(credit_info_df.loc[0, 'undestory_semi_limit'])
        if undestroy_limit + undestroy_limit > 0:
            max_v = max(undestory_used_limit + undestory_semi_overdraft,
                        undestory_avg_use + undestory_semi_avg_overdraft) / (undestroy_limit + undestory_semi_limit)
        else:
            max_v = 0

        count = 0
        df = df.fillna(0)
        for row in df.itertuples():
            if row.repay_amount * 2 > row.amout_replay_amount:
                count = count + 1
        self.variables["credit_financial_tension"] = (count + 1) * min(2, max_v)

    @staticmethod
    def _check_is_null(value):
        return 0 if pd.isnull(value) else value

    # 已激活贷记卡张数
    def _credit_activated_number(self):
        # count(pcredit_loan中report_id=report_id且account_type=04,05且loan_status不等于07,08的记录)
        credit_loan_df = self.cached_data["pcredit_loan"]
        df = credit_loan_df.query('account_type in ["04", "05"] and loan_status not in ["07", "08"]')
        self.variables["credit_activated_number"] = df.shape[0]

    # 贷记卡最低还款张数
    def _credit_min_payed_number(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04,05的记录
        # 2.统计满足条件repay_amount*2>amount_replay_amount的记录
        df = self.cached_data["pcredit_loan"]
        df = df.query('account_type in ["04", "05"]')
        count = 0
        for row in df.itertuples():
            if pd.notna(row.repay_amount) and pd.notna(row.amout_replay_amount):
                if row.repay_amount * 2 > row.amout_replay_amount:
                    count = count + 1
        self.variables["credit_min_payed_number"] = count

    # 贷记卡状态存在"关注"
    def _credit_fiveLevel_c_level_cnt(self):
        # count(pcredit_loan中所有report_id=report_id且account_type=04,05且latest_category=2的记录)
        df = self.cached_data["pcredit_loan"]
        df = df.query('account_type == "04" and category == "2"')
        self.variables["credit_fiveLevel_c_level_cnt"] = df.shape[0]

    # 贷记卡当前逾期次数
    def _credit_now_overdue_cnt(self):
        # 1.从pcredit_loan中选择所有report_id=report_id且account_type=04,05的id;
        # 2.对每一个id,从pcredit_repayment中选取record_id=id且还款时间=report_time前一个月的status;
        # 3.将2中所有status是数字的结果加总
        loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data["pcredit_repayment"]

        loan_df = loan_df.query('account_type in ["04", "05"]')
        if loan_df.empty or repayment_df.empty:
            return

        repayment_df = repayment_df.query('record_id in ' + str(list(loan_df.id)))
        report_time = self.cached_data["report_time"]
        count = 0
        for row in repayment_df.itertuples():
            if (pd.isna(row.status) or not row.status.isdigit()) and row.repayment_amt == 0:
                continue
            if after_ref_date(row.jhi_year, row.month, report_time.year, report_time.month - 1):
                count = count + 1

        self.variables["credit_now_overdue_cnt"] = count

    # 贷记卡历史总逾期次数
    def _credit_total_overdue_cnt(self):
        # 1.从pcredit_loan中选择所有report_id=report_id且account_type=04,05的id;
        # 2.对每一个id,count(pcredit_repayment中record_id=id且repayment_amt>0的记录);
        # 3.将2中所有结果加总
        loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data["pcredit_repayment"]

        loan_df = loan_df.query('account_type in ["04", "05"]')
        if loan_df.empty or repayment_df.empty:
            return

        # repayment_df = repayment_df.query('record_id in ' + str(list(loan_df.id)) + ' and (repayment_amt > 0 or status.str.isdigit())')
        repayment_df = repayment_df[(repayment_df.record_id.isin(list(loan_df.id))) &
                                    ((repayment_df.repayment_amt > 0) |
                                     (repayment_df.status.str.isdigit()))]
        count = repayment_df.shape[0]
        self.variables["credit_total_overdue_cnt"] = count

    #  贷记卡账户状态存在"呆账"
    def _credit_status_bad_cnt(self):
        # count(从pcredit_loan中report_id=report_id且account_type=04,05且loan_status=03的记录)
        loan_df = self.cached_data["pcredit_loan"]
        loan_df = loan_df.query('account_type in ["04", "05"] and loan_status in ["03"]')
        self.variables["credit_status_bad_cnt"] = loan_df.shape[0]

    #  贷记卡账户状态存在"司法追偿"
    def _credit_status_legal_cnt(self):
        # count(从pcredit_loan中report_id=report_id且account_type=04,05且loan_status=8的记录)
        pcredit_loan_df = self.cached_data["pcredit_loan"]
        pcredit_special_df = self.cached_data["pcredit_special"]
        pcredit_loan_df_temp = pcredit_loan_df[pcredit_loan_df['account_type'].isin(['04', '05'])]
        pcredit_special_df_temp = pcredit_special_df[pcredit_special_df['special_type'] == '8']
        df_temp = pd.merge(pcredit_loan_df_temp, pcredit_special_df_temp, left_on='id', right_on='record_id',
                           how='left')
        self.variables['loan_status_legal_cnt'] = df_temp.shape[0]

    #  贷记卡账户状态存在"银行止付、冻结"
    def _credit_status_b_level_cnt(self):
        # count(从pcredit_loan中report_id=report_id且account_type=04,05且loan_status=05,06的记录)
        loan_df = self.cached_data["pcredit_loan"]
        loan_df = loan_df.query('account_type in ["04", "05"] '
                                'and (loan_status == "05" or loan_status == "06")')
        self.variables["credit_status_b_level_cnt"] = loan_df.shape[0]
