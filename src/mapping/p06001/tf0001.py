# -*- coding: utf-8 -*-
# @Time : 2020/2/24 9:23 AM
# @Author : lixiaobo
# @Site : 
# @File : tf0001.py
# @Software: PyCharm
from mapping.tranformer import Transformer


class Tf0001(Transformer):
    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'relent_court_open_owed_owe_laf': '0',  # 联企法院_在营企业_欠款欠费名单命中次数_贷后新增
            'relent_court_open_court_dishonesty_laf': '0',  # 联企法院_在营企业_失信老赖名单命中次数_贷后新增
            'relent_court_open_rest_entry_laf': '0',  # 联企法院_在营企业_限制出入境名单命中次数_贷后新增
            'relent_court_open_high_cons_laf': '0',  # 联企法院_在营企业_限制高消费名单命中次数_贷后新增
            'relent_court_open_cri_sus_laf': '0',  # 联企法院_在营企业_罪犯及嫌疑人名单命中次数_贷后新增
            'relent_court_open_fin_loan_con_laf': '0',  # 联企法院_在营企业_金融借款合同纠纷_贷后新增
            'relent_court_open_loan_con_laf': '0',  # 联企法院_在营企业_借款合同纠纷_贷后新增
            'relent_court_open_pop_loan': '0',  # 联企法院_在营企业_民间借贷纠纷_贷后新增
            'relent_court_open_tax_arrears_laf': '0',  # 联企法院_在营企业_欠税名单命中次数_贷后新增
            'relent_court_open_pub_info_laf': '0',  # 联企法院_在营企业_执行公开信息命中次数_贷后新增
            'relent_court_open_admi_violation_laf': '0',  # 联企法院_在营企业_行政违法记录命中次数_贷后新增
            'relent_court_open_judge_docu_laf': '0',  # 联企法院_在营企业_民商事裁判文书命中次数_贷后新增
            'relent_court_open_judge_proc_laf': '0',  # 联企法院_在营企业_民商事审判流程命中次数_贷后新增
            'relent_court_open_tax_pay_laf': '0',  # 联企法院_在营企业_纳税非正常户命中次数_贷后新增
        }

    def transform(self):
        self.variables["variable_product_code"] = "06001"
