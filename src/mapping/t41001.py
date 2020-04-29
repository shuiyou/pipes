# @Time : 2020/4/23 5:54 PM 
# @Author : lixiaobo
# @File : t41001.py 
# @Software: PyCharm
from mapping.p07001_m.basic_info_processor import BasicInfoProcessor
from mapping.p07001_m.compare_data_processor import CompareDataProcessor
from mapping.p07001_m.credit_info_processor import CreditInfoProcessor
from mapping.p07001_m.data_prepared_processor import DataPreparedProcessor
from mapping.p07001_m.loan_info_processor import LoanInfoProcessor
from mapping.p07001_m.single_info_processor import SingleInfoProcessor
from mapping.p07001_m.unsettle_info_processor import UnSettleInfoProcessor
from mapping.tranformer import Transformer


class T41001(Transformer):
    """
    征信报告决策变量清洗入口及调度中心
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "report_id": 0,  # 报告编号
            "rhzx_business_loan_overdue_cnt": 0,  # 经营性贷款逾期笔数
            "public_sum_count": 0,  # 呆账、资产处置、保证人代偿笔数
            "credit_fiveLevel_a_level_cnt": 0,  # 贷记卡五级分类存在“可疑、损失”
            "loan_fiveLevel_a_level_cnt": 0,  # 贷款五级分类存在“次级、可疑、损失”
            "business_loan_average_overdue_cnt": 0,  # 还款方式为等额本息分期偿还的经营性贷款最大连续逾期期数
            "large_loan_2year_overdue_cnt": 0,  # 经营性贷款（经营性+个人消费大于20万+农户+其他）2年内最大连续逾期期数
            "credit_now_overdue_money": 0,  # 贷记卡当前逾期金额
            "loan_now_overdue_money": 0,  # 贷款当前逾期金额
            "credit_overdue_max_month": 0,  # 贷记卡最大连续逾期月份数
            "single_house_overdue_2year_cnt": 0,  # 单笔房贷近2年内最大逾期次数
            "single_car_overdue_2year_cnt": 0,  # 单笔车贷近2年内最大逾期次数
            "single_consume_overdue_2year_cnt": 0,  # 单笔消费性贷款近2年内最大逾期次数
            "loan_credit_query_3month_cnt": 0,  # 近三个月征信查询（贷款审批及贷记卡审批）次数
            "credit_overdrawn_2card": 0,  # 贷记卡总透支率达80%且存在2张贷记卡最低额还款
            "credit_overdue_5year": 0,  # 总计贷记卡5年内逾期次数
            "loan_consume_overdue_5year": 0,  # 总计消费性贷款（含车贷、房贷、其他消费性贷款）5年内逾期次数
            "credit_max_overdue_2year": 0,  # 单张贷记卡近2年内最大逾期次数
            "unsettled_busLoan_agency_number": 0,  # 有经营性贷款在贷余额的合作机构数
            "loan_credit_small_loan_query_3month_cnt": 0,  # 近三个月小额贷款公司贷款审批查询次数
            "unsettled_consume_agency_cnt": 0,  # 未结清消费性贷款机构数
            "divorce_40_female": 0,  # 年龄>=40,离异或者丧偶，女
            "credit_fiveLevel_b_level_cnt": 0,  # 贷记卡五级分类存在“次级"
            "loan_fiveLevel_b_level_cnt": 0,  # 贷款五级分类存在"关注"
            "loan_scured_five_a_level_abnormality_cnt": 0,  # 对外担保五级分类存在“次级、可疑、损失”
            "extension_number": 0,  # 展期笔数
            "enforce_record": 0,  # 强制执行记录条数
            "unsettled_consume_total_cnt": 0,  # 未结清消费性贷款笔数
            "credit_financial_tension": 0,  # 贷记卡资金紧张程度
            "credit_activated_number": 0,  # 已激活贷记卡张数
            "credit_min_payed_number": 0,  # 贷记卡最低还款张数
            "uncancelled_credit_organization_number": 0,  # 未销户贷记卡发卡机构数
            "credit_fiveLevel_c_level_cnt": 0,  # 贷记卡状态存在"关注"
            "loan_scured_five_b_level_abnormality_cnt": 0,  # 对外担保五级分类存在"关注"
            "unsettled_busLoan_total_cnt": 0,  # 未结清经营性贷款笔笔数
            "marriage_status": 0,  # 离婚
            "judgement_record": 0,  # 民事判决记录数
            "loan_doubtful": 0,  # 疑似压贷笔数
            "guarantee_amont": 0,  # 对外担保金额
            "unsettled_loan_agency_number": 0,  # 未结清贷款机构数
            "unsettled_consume_total_amount": 0,  # 未结清消费性贷款总额
            "tax_record": 0,  # 欠税记录数
            "ad_penalty_record": 0,  # 行政处罚记录数
            "business_loan_overdue_money": 0,  # 经营性贷款逾期金额
            "loan_overdue_2times_cnt": 0,  # 贷款连续逾期2期次数
            "loan_now_overdue_cnt": 0,  # 贷款当前逾期次数
            "loan_total_overdue_cnt": 0,  # 贷款历史总逾期次数
            "loan_max_overdue_month": 0,  # 贷款最大连续逾期
            "credit_now_overdue_cnt": 0,  # 贷记卡当前逾期次数
            "credit_total_overdue_cnt": 0,  # 贷记卡历史总逾期次数
            "single_credit_overdue_cnt_2y": 0,  # 单张贷记卡2年内逾期次数
            "single_house_loan_overdue_cnt_2y": 0,  # 单笔房贷2年内逾期次数
            "single_car_loan_overdue_cnt_2y": 0,  # 单笔车贷2年内逾期次数
            "single_consume_loan_overdue_cnt_2y": 0,  # 单笔消费贷2年内逾期次数
            "total_consume_loan_overdue_cnt_5y": 0,  # 消费贷5年内总逾期次数
            "total_consume_loan_overdue_money_5y": 0,  # 消费贷5年内总逾期金额
            "total_bank_credit_limit": 0,  # 银行授信总额
            "total_bank_loan_balance": 0,  # 银行总余额
            "if_name": 0,  # 与ccs姓名比对
            "phone_alt": 0,  # 与ccs手机号比对
            "if_cert_no": 0,  # 与ccs身份证号比对
            "if_marriage": 0,  # 与ccs婚姻状况比对
            "if_postal_addr": 0,  # 与ccs通讯地址比对
            "if_residence_addr": 0,  # 与ccs户籍地址比对
            "if_live_addr": 0,  # 与ccs居住地址比对
            "if_employee": 0,  # 是否是员工
            "if_official": 0,  # 是否是公检法人员
            "if_spouse_name": 0,  # 与ccs配偶姓名匹配
            "if_spouse_cert_no": 0,  # 与ccs配偶身份证匹配
            "no_loan": 0,  # 名下无贷款无贷记卡
            "house_loan_pre_settle": 0,  # 存在房贷提前结清
            "guar_2times_apply": 0,  # 担保金额是借款金额2倍
            "all_house_car_loan_reg_cnt": 0,  # 所有房屋汽车贷款机构数
            "unsettled_loan_number": 0,  # 未结清贷款笔数
            "unsettled_house_loan_number": 0,  # 未结清房贷笔数
            "loan_approval_year1": 0,  # 贷款审批最近一年内查询次数
            "credit_status_bad_cnt": 0,  # 贷记卡账户状态存在"呆账"
            "credit_status_legal_cnt": 0,  # 贷记卡账户状态存在"司法追偿"
            "credit_status_b_level_cnt": 0,  # 贷记卡账户状态存在"银行止付、冻结"
            "loan_status_bad_cnt": 0,  # 贷款账户状态存在"呆账"
            "loan_status_legal_cnt": 0,  # 贷款账户状态存在"司法追偿"
            "loan_status_b_level_cnt": 0,  # 贷款账户状态存在"银行止付、冻结"
        }

    def transform(self):
        handle_list = [
            DataPreparedProcessor(),
            BasicInfoProcessor(),
            CreditInfoProcessor(),
            LoanInfoProcessor(),
            SingleInfoProcessor(),
            UnSettleInfoProcessor(),
            CompareDataProcessor()
        ]

        for handler in handle_list:
            handler.init(self.variables, self.user_name, self.id_card_no, self.origin_data, self.cached_data)
            handler.process()
