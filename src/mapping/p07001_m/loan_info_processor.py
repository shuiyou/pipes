# @Time : 2020/4/28 2:54 PM 
# @Author : lixiaobo
# @File : loan_info_processor.py 
# @Software: PyCharm
import pandas as pd

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
        self._loan_doubtful()
        self._loan_overdue_2times_cnt()
        self._loan_now_overdue_cnt()
        self._loan_total_overdue_cnt()
        self._loan_max_overdue_month()
        self._loan_status_bad_cnt()  # 贷款账户状态存在"呆账"
        self._loan_status_legal_cnt()  # 贷款账户状态存在"司法追偿"
        self._loan_status_b_level_cnt()  # 贷款账户状态存在"银行止付、冻结"
        self._loan_approval_year1()

    # 贷款五级分类存在“次级、可疑、损失”
    def _loan_fiveLevel_a_level_cnt(self):
        df = self.cached_data.get("pcredit_loan")
        if df is None or df.empty:
            return

        df = df.query('account_type in ["01", "02", "03"] and category in ["3", "4", "5"]')
        self.variables["loan_fiveLevel_a_level_cnt"] = df.shape[0]

    # 贷款当前逾期金额
    def _loan_now_overdue_money(self):
        # 从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03的overdue_amount加总
        credit_loan_df = self.cached_data["pcredit_loan"]
        amt = credit_loan_df.query('account_type in["01", "02"]').dropna(subset=["overdue_amount"])[
            "overdue_amount"].sum()
        self.variables["loan_now_overdue_money"] = amt

    # 近三个月征信查询（贷款审批及贷记卡审批）次数
    def _loan_credit_query_3month_cnt(self):
        # count(pcredit_query_record中report_id=report_id且记录时间在report_time三个月内且=01,02的记录)
        query_record_df = self.cached_data["pcredit_query_record"]

        report_time = self.cached_data["report_time"]
        df = query_record_df.query('reason in["01", "02"]').dropna(subset=["jhi_time"])
        count = 0
        org_list = []
        if not df.empty:
            for index, row in df.iterrows():
                if after_ref_date(row.jhi_time.year, row.jhi_time.month, report_time.year, report_time.month - 3):
                    if row.operator not in org_list:
                        count = count + 1
                        org_list.append(row.operator)
        self.variables["loan_credit_query_3month_cnt"] = count

    # 总计消费性贷款（含车贷、房贷、其他消费性贷款）5年内逾期次数
    def _loan_consume_overdue_5year(self):
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
            self.variables["loan_consume_overdue_5year"] = count

    # 近三个月小额贷款公司贷款审批查询次数
    def _loan_credit_small_loan_query_3month_cnt(self):
        # count(pcredit_query_record中report_id=report_id且记录时间在report_time三个月内且reason=01且operator包含"小额贷款机构"的记录)
        query_record_df = self.cached_data["pcredit_query_record"]
        if not query_record_df.empty:
            query_record_df = query_record_df[query_record_df["operator"].str.contains("小额贷款")]
            query_record_df = query_record_df.query('reason == "01"')
            report_time = self.cached_data["report_time"]
            count = 0
            for index, row in query_record_df.iterrows():
                if after_ref_date(row.jhi_time.year, row.jhi_time.month, report_time.year, report_time.month - 3):
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

    # 疑似压贷笔数
    def _loan_doubtful(self):
        # "1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且((loan_type=01,07,99或者(loan_type=04且loan_amount>200000))或者guarantee_type=3)的记录
        # 2.针对1中每一个不同account_org,统计该机构每个月放款笔数,若月最大放款笔数>=3,标记为随借随还机构,跳过
        # 3.2不满足条件2的话,该机构5年内放款笔数>=3,计算最新一笔贷款总额和次新一笔贷款总额的比值,如果比值小于0.8,则变量+1"
        loan_df = self.cached_data["pcredit_loan"]
        loan_df = loan_df.query('account_type in ["01", "02", "03"] and '
                                '(loan_type in ["01", "07", "99"] '
                                'or (loan_type == "04" and loan_amount > 200000) '
                                'or loan_guarantee_type == "3")')

        loan_df = loan_df.filter(items=["account_org", "loan_date", "loan_amount"])
        loan_df = loan_df[pd.notna(loan_df["loan_date"])]
        loan_df["year"] = loan_df["loan_date"].transform(lambda x: x.year)
        loan_df["month"] = loan_df["loan_date"].transform(lambda x: x.month)
        loan_df["account_org"] = loan_df["account_org"].transform(lambda x: x.replace('"', ""))

        final_count = 0
        report_time = self.cached_data["report_time"]
        df = loan_df.groupby(["account_org", "year", "month"])["month"].count().reset_index(name="count")
        ignore_org_list = df.query('count >= 3')["account_org"].unique()

        all_org_list = loan_df.loc[:, "account_org"].unique()
        loan_doubtful_org = []

        for org_name in all_org_list:
            if org_name in ignore_org_list:
                continue
            express = 'account_org == "' + org_name + \
                      '" and (year > ' + str(report_time.year - 5) \
                      + ' or (year == ' + str(report_time.year - 5) \
                      + ' and month >= ' + str(report_time.month) + '))'

            item_df = loan_df.query(express)
            if item_df.shape[0] < 3:
                continue

            item_df = item_df.sort_values(by=["year", "month"], ascending=False)
            first_amt = item_df.iloc[0].loan_amount
            second_amt = item_df.iloc[1].loan_amount
            if first_amt and second_amt:
                ratio = first_amt / second_amt
                if ratio < 0.8:
                    loan_doubtful_org.append(org_name)
                    final_count = final_count + 1

        self.variables["loan_doubtful"] = final_count
        self.variables["loan_doubtful_org"] = loan_doubtful_org

    # 贷款连续逾期2期次数
    def _loan_overdue_2times_cnt(self):
        # 1.从pcredit_loan中选择所有report_id=report_id且account_type=01,02,03的id;
        # 2.对每一个id,count(pcredit_repayment中record_id=id且status=2的记录);
        # 3.2中所有结果加总
        loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data["pcredit_repayment"]
        loan_df = loan_df.query('account_type in ["01", "02", "03"]')
        if loan_df.empty or repayment_df.empty:
            return
        repayment_df = repayment_df.query('record_id in ' + str(list(loan_df.id)) + ' and status == "2"')
        self.variables["loan_overdue_2times_cnt"] = repayment_df.shape[0]

    # 贷款当前逾期次数
    def _loan_now_overdue_cnt(self):
        # 1.从pcredit_loan中选择所有report_id=report_id且account_type=01,02,03的id;
        # 2.对每一个id,从pcredit_repayment中选取record_id=id且还款时间=report_time前一个月的status;
        # 3.将2中所有status是数字的结果加总
        loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data["pcredit_repayment"]
        loan_df = loan_df.query('account_type in ["01", "02", "03"]')
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
        self.variables["loan_now_overdue_cnt"] = count

    # 贷款历史总逾期次数
    def _loan_total_overdue_cnt(self):
        # 1.从pcredit_loan中选择所有report_id=report_id且account_type=01,02,03的id;
        # 2.对每一个id,count(pcredit_repayment中record_id=id且repayment_amt>0的记录);
        # 3.将2中所有结果加总
        loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data["pcredit_repayment"]
        loan_df = loan_df.query('account_type in ["01", "02", "03"]')
        if loan_df.empty or repayment_df.empty:
            return
        repayment_df["status"] = repayment_df["status"].fillna("")
        # repayment_df = repayment_df.query('record_id in ' + str(list(loan_df.id))
        #                                   + ' and (repayment_amt > 0 or status.str.isdigit())')
        repayment_df = repayment_df[(repayment_df.record_id.isin(list(loan_df.id))) &
                                    ((repayment_df.repayment_amt > 0) |
                                     (repayment_df.status.str.isdigit()))]
        self.variables["loan_total_overdue_cnt"] = repayment_df.shape[0]

    # 贷款最大连续逾期
    def _loan_max_overdue_month(self):
        # 1.从pcredit_loan中选择所有report_id=report_id且account_type=01,02,03的id;
        # 2.对每一个id,max(pcredit_repayment中record_id=id且status是数字的记录);
        # 3.2中所有结果取最大值
        loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data["pcredit_repayment"]
        loan_df = loan_df.query('account_type in ["01", "02", "03"]')
        if loan_df.empty or repayment_df.empty:
            return

        repayment_df = repayment_df.query('record_id in ' + str(list(loan_df.id)))
        status_series = repayment_df["status"]
        status_series = status_series.transform(lambda x: 0 if pd.isna(x) or not x.isdigit() else int(x))
        count = 0 if len(status_series) == 0 else status_series.max()

        self.variables["loan_max_overdue_month"] = count

    #  贷款账户状态存在"呆账"
    def _loan_status_bad_cnt(self):
        # count(从pcredit_loan中report_id=report_id且account_type=01,02,03且loan_status=03的记录)
        self._loan_count(["01", "02", "03"], ["03"], "loan_status_bad_cnt")

    #  贷款账户状态存在"司法追偿"
    def _loan_status_legal_cnt(self):
        # count(从pcredit_loan中report_id=report_id且account_type=01,02,03且loan_status=8的记录)
        pcredit_loan_df = self.cached_data["pcredit_loan"]
        pcredit_special_df = self.cached_data["pcredit_special"]
        pcredit_loan_df_temp = pcredit_loan_df[pcredit_loan_df['account_type'].isin(['01', '02', '03'])]
        pcredit_special_df_temp = pcredit_special_df[pcredit_special_df['special_type'] == '8']
        df_temp = pd.merge(pcredit_special_df_temp, pcredit_loan_df_temp, left_on='record_id',
                           right_on='id', how='left')
        self.variables['loan_status_legal_cnt'] = df_temp.shape[0]

    #  贷款账户状态存在"银行止付、冻结"
    def _loan_status_b_level_cnt(self):
        # count(从pcredit_loan中report_id=report_id且account_type=01,02,03且loan_status=05,06的记录)
        loan_df = self.cached_data["pcredit_loan"]
        loan_df = loan_df.query('account_type in ["01", "02", "03"] '
                                'and (loan_status == "05" or loan_status == "06")')
        self.variables["loan_status_b_level_cnt"] = loan_df.shape[0]

    def _loan_count(self, account_types, loan_status, var_name):
        loan_df = self.cached_data["pcredit_loan"]
        loan_df = loan_df.query('account_type in ' + str(account_types) + ' and loan_status in ' + str(loan_status))
        self.variables[var_name] = loan_df.shape[0]

    # 贷款审批最近一年内查询次数
    def _loan_approval_year1(self):
        # count(从pcredit_query_record中report_id=report_id且reason=01且jhi_time>report_time前一年的记录)
        report_time = self.cached_data["report_time"]
        query_record_df = self.cached_data["pcredit_query_record"]
        query_record_df = query_record_df.query('reason == "01"')

        count = 0
        for row in query_record_df.itertuples():
            if pd.notna(row.jhi_time):
                if after_ref_date(row.jhi_time.year, row.jhi_time.month, report_time.year - 1, report_time.month):
                    count = count + 1
        self.variables["loan_approval_year1"] = count
