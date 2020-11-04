# @Time : 2020/4/23 7:36 PM 
# @Author : lixiaobo
# @File : v41001.py.py 
# @Software: PyCharm
from mapping.p07001_m.data_prepared_processor import DataPreparedProcessor
from mapping.tranformer import Transformer
from view.p07001_v.basic_info_view import BasicInfoView
from view.p07001_v.pcredit_acc_speculate_view import PcreditAccSpeculateView
from view.p07001_v.pcredit_biz_info_view import PcreditBizInfoView
from view.p07001_v.pcredit_default_info_view import PcreditDefaultInfoView
from view.p07001_v.pcredit_live_view import PcreditLiveView
from view.p07001_v.pcredit_loan_view import PcreditLoanView
from view.p07001_v.pcredit_person_info_view import PcreditPersonInfoView
from view.p07001_v.pcredit_phone_his_view import PcreditPhoneHisView
from view.p07001_v.pcredit_portraits_new_view import PcreditPortraitsNewView
from view.p07001_v.pcredit_query_record_view import PcreditQueryRecordView
from view.p07001_v.pcredit_special_view import PcreditSpecialView


class V41001(Transformer):
    """
    征信报告变量清洗
    """
    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "name": "",
            "certificate_no": "",
            "report_no": "",
            "report_time": "",
            "credit_report_no": "",
            "credit_report_time": "",
            "sex": "",
            "phone": "",
            "marriage_status": "",
            "employment": "",
            "work_unit": "",
            "communication_address": "",
            "residence_address": "",
            "live_address": "",
            "live_address_type": "",
            "mort_settle_loan_date": "",
            "mort_no_settle_loan_date": "",
            "spouse_name": "",
            "spouse_certificate_no": "",
            "spouse_phone": "",
            "extension_number": "",
            "category": "",
            "default_type": [],
            "rhzx_business_loan_overdue_cnt": "",
            "business_loan_overdue_money": "",
            "loan_overdue_2times_cnt": "",
            "loan_now_overdue_cnt": "",
            "loan_now_overdue_money": "",
            "loan_total_overdue_cnt": "",
            "loan_max_overdue_month": "",
            "credit_now_overdue_cnt": "",
            "credit_now_overdue_money": "",
            "credit_total_overdue_cnt": "",
            "credit_overdue_max_month": "",
            "single_credit_overdue_cnt_2y": "",
            "single_house_loan_overdue_cnt_2y": "",
            "single_car_loan_overdue_cnt_2y": "",
            "single_consume_loan_overdue_cnt_2y": "",
            "total_consume_loan_overdue_cnt_5y": "",
            "total_consume_loan_overdue_money_5y": "",
            "biz_firstmonth": "",
            "total_bank_credit_limit": "",
            "total_bank_loan_balance": "",
            "total_repay_6m_before": "",
            "repay_loan_6m_before": "",
            "repay_principal_6m_before": "",
            "repay_installment_6m_before": "",
            "repay_credit_6m_before": "",
            "total_repay_5m_before": "",
            "repay_loan_5m_before": "",
            "repay_principal_5m_before": "",
            "repay_installment_5m_before": "",
            "repay_credit_5m_before": "",
            "total_repay_4m_before": "",
            "repay_loan_4m_before": "",
            "repay_principal_4m_before": "",
            "repay_installment_4m_before": "",
            "repay_credit_4m_before": "",
            "total_repay_3m_before": "",
            "repay_loan_3m_before": "",
            "repay_principal_3m_before": "",
            "repay_installment_3m_before": "",
            "repay_credit_3m_before": "",
            "total_repay_2m_before": "",
            "repay_loan_2m_before": "",
            "repay_principal_2m_before": "",
            "repay_installment_2m_before": "",
            "repay_credit_2m_before": "",
            "total_repay_1m_before": "",
            "repay_loan_1m_before": "",
            "repay_principal_1m_before": "",
            "repay_installment_1m_before": "",
            "repay_credit_1m_before": "",
            "total_repay_1m_after": "",
            "repay_loan_1m_after": "",
            "repay_principal_1m_after": "",
            "repay_installment_1m_after": "",
            "repay_credit_1m_after": "",
            "total_repay_2m_after": "",
            "repay_loan_2m_after": "",
            "repay_principal_2m_after": "",
            "repay_installment_2m_after": "",
            "repay_credit_2m_after": "",
            "total_repay_3m_after": "",
            "repay_loan_3m_after": "",
            "repay_principal_3m_after": "",
            "repay_installment_3m_after": "",
            "repay_credit_3m_after": "",
            "total_repay_4m_after": "",
            "repay_loan_4m_after": "",
            "repay_principal_4m_after": "",
            "repay_installment_4m_after": "",
            "repay_credit_4m_after": "",
            "total_repay_5m_after": "",
            "repay_loan_5m_after": "",
            "repay_principal_5m_after": "",
            "repay_installment_5m_after": "",
            "repay_credit_5m_after": "",
            "total_repay_6m_after": "",
            "repay_loan_6m_after": "",
            "repay_principal_6m_after": "",
            "repay_installment_6m_after": "",
            "repay_credit_6m_after": "",
            "total_repay_7m_after": "",
            "repay_loan_7m_after": "",
            "repay_principal_7m_after": "",
            "repay_installment_7m_after": "",
            "repay_credit_7m_after": "",
            "total_repay_8m_after": "",
            "repay_loan_8m_after": "",
            "repay_principal_8m_after": "",
            "repay_installment_8m_after": "",
            "repay_credit_8m_after": "",
            "total_repay_9m_after": "",
            "repay_loan_9m_after": "",
            "repay_principal_9m_after": "",
            "repay_installment_9m_after": "",
            "repay_credit_9m_after": "",
            "total_repay_10m_after": "",
            "repay_loan_10m_after": "",
            "repay_principal_10m_after": "",
            "repay_installment_10m_after": "",
            "repay_credit_10m_after": "",
            "total_repay_11m_after": "",
            "repay_loan_11m_after": "",
            "repay_principal_11m_after": "",
            "repay_installment_11m_after": "",
            "repay_credit_11m_after": "",
            "total_repay_12m_after": "",
            "repay_loan_12m_after": "",
            "repay_principal_12m_after": "",
            "repay_installment_12m_after": "",
            "repay_credit_12m_after": "",
            "average_repay_6m_before": "",
            "average_repay_12m_after": "",
            "each_loan_account_org": "",
            "each_loan_date": "",
            "each_principal_amount": "",
            "each_interest_rate": "",
            "each_loan_type": "",
            "each_loan_status": "",
            "max_principal_amount": "",
            "min_principal_amount": "",
            "rng_principal_amount": "",
            "multiple_principal_amount": "",
            "loan_principal_0_20w_cnt": "",
            "loan_principal_20_50w_cnt": "",
            "loan_principal_50_100w_cnt": "",
            "loan_principal_100_200w_cnt": "",
            "loan_principal_200w_cnt": "",
            "loan_principal_total_cnt": "",
            "loan_principal_0_20w_prop": "",
            "loan_principal_20_50w_prop": "",
            "loan_principal_50_100w_prop": "",
            "loan_principal_100_200w_prop": "",
            "loan_principal_200w_prop": "",
            "busi_loan_date": "",
            "busi_loan_balance": "",
            "loan_type": "",
            "loan_type_balance": "",
            "loan_type_cnt": "",
            "loan_type_balance_prop": "",
            "guar_type": "",
            "guar_type_balance": "",
            "guar_type_cnt": "",
            "guar_type_balance_prop": "",
            "ensure_max_principal": "",
            "mort_max_principal": "",
            "ensure_principal_multi_apply": "",
            "mort_principal_multi_apply": "",
            "new_org_3m_ago": "",
            "loan_type_3m_ago": "",
            "principal_amount_3m_ago": "",
            "new_org_6m_ago": "",
            "loan_type_6m_ago": "",
            "principal_amount_6m_ago": "",
            "new_org_12m_ago": "",
            "loan_type_12m_ago": "",
            "principal_amount_12m_ago": "",
            "busi_org_cnt_2y_ago": "",
            "busi_org_balance_2y_ago": "",
            "busi_org_cnt_1y_ago": "",
            "busi_org_balance_1y_ago": "",
            "account_org": "",
            "total_principal_3y_ago": "",
            "max_interest_rate_3y_ago": "",
            "max_terms_3y_ago": "",
            "total_principal_2y_ago": "",
            "max_interest_rate_2y_ago": "",
            "max_terms_2y_ago": "",
            "total_principal_1y_ago": "",
            "max_interest_rate_1y_ago": "",
            "max_terms_1y_ago": "",
            "credit_org": "",
            "credit_loan_date": "",
            "credit_loan_status": "",
            "credit_principal_amount": "",
            "credit_quota_used": "",
            "credit_avg_used_6m": "",
            "credit_usage_rate": "",
            "credit_min_repay": "",
            "credit_org_cnt": "",
            "total_credit_card_limit": "",
            "total_credit_quota_used": "",
            "total_credit_avg_used_6m": "",
            "total_credit_usage_rate": "",
            "credit_min_repay_cnt": "",
            "total_credit_limit_3y_ago": "",
            "total_credit_cnt_3y_ago": "",
            "total_credit_limit_2y_ago": "",
            "total_credit_cnt_2y_ago": "",
            "total_credit_limit_1y_ago": "",
            "total_credit_cnt_1y_ago": "",
            "guar_acc_org": [],
            "guar_loan_type": [],
            "guar_end_date": [],
            "guar_principal_amount": [],
            "guar_loan_balance": [],
            "guar_latest_category": [],
            "guar_acc_org_cnt": "",
            "total_guar_principal_amount": "",
            "total_guar_loan_balance": "",
            "jhi_time_3m": "",
            "operator_3m": "",
            "reason_3m": "",
            "guar_query_cnt": "",
            "loan_query_cnt": "",
            "self_query_cnt": "",
            "jhi_time_1y": "",
            "operator_1y": "",
            "reason_1y": "",
            "if_loan": "",
            "bank_query_cnt": "",
            "bank_query_loan_cnt": "",
            "credit_query_cnt": "",
            "credit_query_loan_cnt": "",
            "hint_account_org": "",
            "hint_loan_date": "",
            "hint_principal_amount": "",
            "busi_org_cnt_3y_ago": "",
            "busi_org_balance_3y_ago": "",
            "busi_org_cnt_now": "",
            "busi_org_balance_now": "",
            "settle_account_org": "",
            "settle_loan_date": "",
            "settle_date": "",
            "settle_loan_amount": "",
            "busi_loan_balance_max": "",
            "busi_loan_balance_min": "",
            "busi_org_balance_3y_ago_max": "",
            "busi_org_balance_3y_ago_min": "",
            "busi_org_balance_2y_ago_max": "",
            "busi_org_balance_2y_ago_min": "",
            "busi_org_balance_1y_ago_max": "",
            "busi_org_balance_1y_ago_min": "",
            "busi_org_balance_now_max": "",
            "busi_org_balance_now_min": "",
            "house_loan_pre_settle_org": [],
            "house_loan_pre_settle_date": [],
            "if_work_unit": 0,
            "total_credit_used_rate": 0,
            "total_credit_min_repay_cnt": 0,
            "loan_doubtful_org": 0
        }

    def transform(self):
        view_handle_list = [
            BasicInfoView(),
            PcreditPersonInfoView(),
            PcreditBizInfoView(),
            PcreditDefaultInfoView(),
            PcreditLiveView(),
            PcreditLoanView(),
            PcreditPhoneHisView(),
            PcreditPortraitsNewView(),
            PcreditQueryRecordView(),
            PcreditSpecialView(),
            PcreditAccSpeculateView()
        ]

        for view in view_handle_list:
            view.init(self.variables, self.user_name, self.id_card_no, self.origin_data, self.cached_data)
            view.process()