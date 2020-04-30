# @Time : 2020/4/23 7:06 PM 
# @Author : lixiaobo
# @File : basic_info_handle.py.py 
# @Software: PyCharm
import pandas as pd

from mapping.module_processor import ModuleProcessor


# 基本信息处理
from product.date_time_util import after_ref_date
from util.id_card_info import GetInformation


class BasicInfoProcessor(ModuleProcessor):

    def process(self):
        print("BasicInfoProcessor process")
        self.variables["report_id"] = self.cached_data["report_id"]

        self._rhzx_business_loan_overdue_cnt()
        self._public_sum_count()
        self._business_loan_average_overdue_cnt()
        self._large_loan_2year_overdue_cnt()
        self._divorce_40_female()
        self._extension_number()
        self._enforce_record()
        self._marriage_status()

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

    # 经营性贷款（经营性+个人消费大于20万+农户+其他）2年内最大连续逾期期数
    def _large_loan_2year_overdue_cnt(self):
        '''
        "1.从pcredit_loan中选择所有report_id=report_id且account_type=01,02,03且(loan_type=01,07,99或者(loan_type=04且principal_amount>200000))的id,
        2.对于每一个id,max(pcredit_payment中所有record_id=id且status是数字且还款时间在report_time两年内的status),
        3.从2中所有结果中选取最大的一个"
        '''
        credit_loan_df = self.cached_data.get("pcredit_loan")
        repayment_df = self.cached_data.get("pcredit_repayment")

        if credit_loan_df is None or credit_loan_df.empty or repayment_df is None or repayment_df.empty:
            return

        credit_loan_df = credit_loan_df.query('account_type in ["01", "02", "03"] and (loan_type in ["01", "07", '
                                              '"99"] or (loan_type == "04" and principal_amount>200000))')

        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        report_time = self.cached_data["report_time"]
        if repayment_df is not None:
            status_list = []
            for index, row in repayment_df.iterrows():
                if row["status"] and row["status"].isdigit():
                    if after_ref_date(row.jhi_year, row.month, report_time.year-2, report_time.month):
                        status_list.append(int(row["status"]))
            self.variables["business_loan_average_overdue_cnt"] = 0 if len(status_list) == 0 else max(status_list)

    # 年龄>=40,离异或者丧偶，女
    def _divorce_40_female(self):
        # 1.count(pcredit_person_info中report_id=report_id且sex=2且marriage_status=3,4的记录)
        # 2.count(ccs.cus_indiv中cus_name=name且indiv_sex=2且cert_code=certificate_no且marital_status=12,13的记录)
        # 3.从credit_base_info中找到report_id对应的certificate_no,用身份证号第7到14位出生日期算出年龄
        # 4.若(1中count+2中count)>0且3中年龄>=40则变量=1,否则=0
        id_card_no = self.cached_data["id_card_no"]
        information = GetInformation(id_card_no)
        if information.get_sex() != 2:
            return
        if information.get_age() < 40:
            return

        marry_state = self.cached_data.get("basicMarryState")
        if marry_state and marry_state in ["DIVORCE", "WIDOWHOOD"]:
            self.variables["divorce_40_female"] = 1
            return

        credit_person_info = self.cached_data["pcredit_person_info"]
        credit_person_info = credit_person_info.query('sex == "2" and marriage_status == "3"')
        self.variables["divorce_40_female"] = 0 if credit_person_info.empty else 1

    # 展期笔数
    def _extension_number(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03的id
        # 2.对每一个id,若count(pcredit_special中record_id=id且special_type=1的记录)>0则变量+1

        credit_loan_df = self.cached_data["pcredit_loan"]
        credit_special_df = self.cached_data["pcredit_special"]

        if credit_loan_df.empty or credit_special_df.empty:
            return

        credit_loan_df = credit_loan_df.query('account_type in ["01", "02", "03"]')
        if credit_loan_df.empty:
            return

        count = 0
        for index, row in credit_loan_df.iterrows():
            df = credit_special_df.query('record_id == ' + str(row.id) + ' and special_type == 1')
            if not df.empty:
                count = count + 1
        self.variables["extension_number"] = count

    # 强制执行记录条数
    def _enforce_record(self):
        # count(pcredit_force_execution_record中report_id=report_id的记录)
        df = self.cached_data["pcredit_force_execution_record"]
        self.variables["enforce_record"] = df.shape[0]

    # 离婚
    def _marriage_status(self):
        # 1.count(pcredit_person_info中report_id=report_id且marriage_status=3的记录)
        # 2.count(ccs.cus_indiv中cus_name=name且cert_code=certificate_no且marital_status=12的记录)
        # 3.若(1中count+2中count)>0则变量=1,否则=0

        marry_state = self.cached_data.get("basicMarryState")
        if marry_state and marry_state == "DIVORCE":
            self.variables["marriage_status"] = 1
            return

        credit_person_df = self.cached_data["pcredit_person_info"]
        count = credit_person_df.query('marriage_status == 3').shape[0]

        self.variables["marriage_status"] = 1 if count > 0 else 0

