# coding: utf-8
from sqlalchemy import Column, DECIMAL, Date, DateTime, ForeignKey, Index, JSON, String, TIMESTAMP, Table, Text, text
from sqlalchemy.dialects.mysql import BIGINT, BIT, INTEGER, LONGTEXT, MEDIUMTEXT, TINYINT
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class AttachmentInfo(Base):
    __tablename__ = 'attachment_info'

    id = Column(BIGINT(20), primary_key=True)
    file_url = Column(String(255))
    file_key = Column(String(255))
    file_type = Column(String(50))
    file_raw_txt = Column(LONGTEXT)
    file_desc = Column(String(64))
    create_time = Column(DateTime)


class BizRequest(Base):
    __tablename__ = 'biz_request'

    id = Column(BIGINT(20), primary_key=True)
    req_src = Column(String(50))
    biz_type = Column(Text)
    req_no = Column(String(32))
    step_req_no = Column(String(32))
    req_msg = Column(MEDIUMTEXT)
    resp_msg = Column(Text)
    status = Column(String(50))
    create_time = Column(DateTime)
    modify_time = Column(DateTime)


class CfgApiPriority(Base):
    __tablename__ = 'cfg_api_priority'

    id = Column(BIGINT(20), primary_key=True)
    biz_no = Column(String(64))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    api_id = Column(BIGINT(20))
    api_no = Column(String(20))
    api_name = Column(String(64))
    priority = Column(INTEGER(11))
    enabled = Column(BIT(1))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class Channel(Base):
    __tablename__ = 'channel'

    id = Column(BIGINT(20), primary_key=True)
    channel_no = Column(String(20), unique=True)
    name = Column(String(20))
    base_url = Column(String(100))
    base_param = Column(Text)
    channel_status = Column(TINYINT(1))


class ChannelApi(Base):
    __tablename__ = 'channel_api'

    id = Column(BIGINT(20), primary_key=True)
    api_no = Column(String(20), unique=True)
    channel_no = Column(String(20))
    name = Column(String(20))
    path = Column(String(100))
    method = Column(String(100))
    charge = Column(BIT(1))
    api_config = Column(Text)
    data_valid_days = Column(BIGINT(20))
    biz_type = Column(String(10))
    req_msg_check_field = Column(String(200))
    priority = Column(INTEGER(2))
    api_status = Column(TINYINT(1))


class CompayCreditCode(Base):
    __tablename__ = 'compay_credit_code'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    ent_name = Column(String(64))
    credit_code = Column(String(32))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime, nullable=False)
    create_time = Column(DateTime)
    modify_time = Column(DateTime)


class CreditBaseInfo(Base):
    __tablename__ = 'credit_base_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    report_type = Column(String(32))
    credit_type = Column(String(16))
    name = Column(String(20))
    certificate_type = Column(String(16))
    certificate_no = Column(String(32))
    queryer = Column(String(50))
    query_reason = Column(String(50))
    query_time = Column(DateTime)
    report_time = Column(DateTime)
    create_user = Column(String(10))
    create_time = Column(DateTime)
    create_dep = Column(String(10))
    update_user = Column(String(10))
    update_time = Column(DateTime)
    update_dep = Column(String(10))
    file_path = Column(String(200))
    json_id = Column(String(32))
    report_no = Column(String(32))


class CreditParseRequest(Base):
    __tablename__ = 'credit_parse_request'

    id = Column(BIGINT(20), primary_key=True)
    app_id = Column(String(32))
    attachment_id = Column(BIGINT(20))
    out_req_no = Column(String(32))
    out_apply_no = Column(String(32))
    provider = Column(String(50))
    credit_type = Column(String(50))
    credit_version = Column(String(50))
    biz_req_no = Column(String(32))
    report_id = Column(String(32))
    resp_data = Column(LONGTEXT)
    process_status = Column(String(50))
    process_memo = Column(String(512))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


t_databasechangelog = Table(
    'databasechangelog', metadata,
    Column('ID', String(255), nullable=False),
    Column('AUTHOR', String(255), nullable=False),
    Column('FILENAME', String(255), nullable=False),
    Column('DATEEXECUTED', DateTime, nullable=False),
    Column('ORDEREXECUTED', INTEGER(11), nullable=False),
    Column('EXECTYPE', String(10), nullable=False),
    Column('MD5SUM', String(35)),
    Column('DESCRIPTION', String(255)),
    Column('COMMENTS', String(255)),
    Column('TAG', String(255)),
    Column('LIQUIBASE', String(20)),
    Column('CONTEXTS', String(255)),
    Column('LABELS', String(255)),
    Column('DEPLOYMENT_ID', String(10))
)


class Databasechangeloglock(Base):
    __tablename__ = 'databasechangeloglock'

    ID = Column(INTEGER(11), primary_key=True)
    LOCKED = Column(BIT(1), nullable=False)
    LOCKGRANTED = Column(DateTime)
    LOCKEDBY = Column(String(255))


class ImportTemplateDetail(Base):
    __tablename__ = 'import_template_details'

    id = Column(BIGINT(20), primary_key=True)
    template_id = Column(String(32))
    template_title = Column(String(64))
    flow_title = Column(String(32))
    created_by = Column(String(50), nullable=False)
    created_date = Column(DateTime, nullable=False)
    last_modified_by = Column(String(50))
    last_modified_date = Column(DateTime)


class ImportTemplateFlow(Base):
    __tablename__ = 'import_template_flow'

    id = Column(BIGINT(20), primary_key=True)
    template_id = Column(String(32))
    template_name = Column(String(50))
    title_count = Column(BIGINT(20))
    template_type = Column(String(5))
    in_come_out_pay = Column(String(2))
    in_come_custom = Column(String(30))
    out_pay_custom = Column(String(30))
    created_by = Column(String(50), nullable=False)
    created_date = Column(DateTime, nullable=False)
    last_modified_by = Column(String(50))
    last_modified_date = Column(DateTime)


class InfoAntiFraud(Base):
    __tablename__ = 'info_anti_fraud'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    user_name = Column(String(64))
    id_card_no = Column(String(64))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime, nullable=False)
    phone = Column(String(15))
    result_code = Column(String(6))
    final_decision = Column(String(50))
    create_time = Column(DateTime)
    anti_fraud_id = Column(BIGINT(20))


class InfoAntiFraudRule(Base):
    __tablename__ = 'info_anti_fraud_rule'

    id = Column(BIGINT(20), primary_key=True)
    anti_fraud_rule_id = Column(BIGINT(20))
    rule_name = Column(String(32))
    rule_score = Column(INTEGER(11))
    rule_decision = Column(String(255))
    rule_memo = Column(String(128))
    rule_id = Column(String(32))


class InfoAntiFraudStrategy(Base):
    __tablename__ = 'info_anti_fraud_strategy'

    id = Column(BIGINT(20), primary_key=True)
    anti_fraud_id = Column(BIGINT(20))
    strategy_name = Column(String(32))
    strategy_decision = Column(String(255))
    strategy_mode = Column(String(32))
    risk_type = Column(String(32))
    strategy_id = Column(String(32))
    anti_fraud_rule_id = Column(BIGINT(20))


class InfoBankAccount(Base):
    __tablename__ = 'info_bank_account'

    id = Column(BIGINT(20), primary_key=True)
    account_name = Column(String(50))
    account_id = Column(String(30))
    card_type = Column(String(50))
    id_no = Column(String(20))
    id_type = Column(String(50))
    jhi_type = Column(String(5))
    file_path = Column(String(255))
    created_by = Column(String(50), nullable=False)
    created_date = Column(DateTime, nullable=False)
    last_modified_by = Column(String(50))
    last_modified_date = Column(DateTime)


class InfoBankFlow(Base):
    __tablename__ = 'info_bank_flow'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(String(30))
    mobile = Column(String(20))
    template_id = Column(String(32))
    balance = Column(DECIMAL(16, 4))
    currency = Column(String(20))
    transaction_time = Column(DateTime)
    transaction_amount = Column(DECIMAL(16, 4))
    transaction_channel = Column(String(50))
    transaction_type = Column(String(200))
    transaction_use = Column(String(200))
    flow_no = Column(String(50))
    flow_type = Column(String(50))
    opponent_name = Column(String(50))
    opponent_account_bank = Column(String(128))
    opponent_account = Column(String(50))
    batch_no = Column(String(50))
    status = Column(String(50))
    is_true = Column(BIT(1))
    remark = Column(String(200))
    created_by = Column(String(50), nullable=False)
    created_date = Column(DateTime, nullable=False)
    last_modified_by = Column(String(50))
    last_modified_date = Column(DateTime)


class InfoBlackList(Base):
    __tablename__ = 'info_black_list'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    user_name = Column(String(64))
    id_card_no = Column(String(64))
    user_type = Column(String(50))
    card_type = Column(String(50))
    reason = Column(Text)
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    valid = Column(TINYINT(4), server_default=text("'1'"))
    expired_at = Column(DateTime)
    create_time = Column(DateTime)
    modify_time = Column(DateTime)


class InfoCallbackTemp(Base):
    __tablename__ = 'info_callback_temp'

    id = Column(BIGINT(20), primary_key=True)
    cus_name = Column(String(30), nullable=False)
    cus_code = Column(String(20), nullable=False)
    cus_type = Column(String(22), nullable=False)
    flag = Column(String(150), nullable=False)
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    query_param = Column(JSON)
    create_time = Column(DateTime)


class InfoCertification(Base):
    __tablename__ = 'info_certification'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    user_name = Column(String(64))
    phone = Column(String(15))
    id_card_no = Column(String(64))
    bank_card_no = Column(String(64))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    certification_type = Column(String(50))
    expired_at = Column(DateTime, nullable=False)
    result = Column(BIT(1))
    cause = Column(String(64))
    create_time = Column(DateTime)
    modify_time = Column(DateTime)


class InfoComBusAlter(Base):
    __tablename__ = 'info_com_bus_alter'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    alt_item = Column(String(200))
    alt_be = Column(Text)
    alt_af = Column(Text)
    alt_date = Column(DateTime)
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusAnReportBasic(Base):
    __tablename__ = 'info_com_bus_an_report_basic'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    anche_id = Column(String(300))
    addr = Column(String(100))
    anche_date = Column(DateTime)
    anche_year = Column(INTEGER(11))
    bus_status = Column(String(255))
    credit_no = Column(String(32))
    email = Column(String(32))
    ent_name = Column(String(64))
    postal_code = Column(String(32))
    reg_no = Column(String(32))
    tel = Column(String(15))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusAnReportChange(Base):
    __tablename__ = 'info_com_bus_an_report_change'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(BIGINT(20))
    anche_id = Column(String(300))
    al_item = Column(Text)
    alt_af = Column(Text)
    alt_be = Column(Text)
    alt_date = Column(DateTime)


class InfoComBusAnReportContribution(Base):
    __tablename__ = 'info_com_bus_an_report_contribution'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(BIGINT(20))
    anche_id = Column(String(300))
    con_date = Column(DateTime)
    con_form = Column(String(64))
    currency = Column(String(50))
    name = Column(String(500))
    li_sub_conam = Column(DECIMAL(16, 4))


class InfoComBusAnReportForeign(Base):
    __tablename__ = 'info_com_bus_an_report_foreign'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(BIGINT(20))
    anche_id = Column(String(300))
    credit_no = Column(String(64))
    ent_name = Column(String(64))
    reg_no = Column(String(32))


class InfoComBusAnReportGuarantee(Base):
    __tablename__ = 'info_com_bus_an_report_guarantee'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(BIGINT(20))
    anche_id = Column(String(300))
    ga_type = Column(String(255))
    gua_rap_period = Column(String(255))
    more = Column(String(64))
    mort_ga_gor = Column(String(64))
    pef_per_form = Column(DateTime)
    pef_per_to = Column(DateTime)
    pri_clase_cam = Column(DECIMAL(16, 4))
    pri_clasec_kind = Column(String(255))
    rage = Column(Text)


class InfoComBusAnReportInvestment(Base):
    __tablename__ = 'info_com_bus_an_report_investment'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(BIGINT(20))
    anche_id = Column(String(300))
    con_date = Column(DateTime)
    con_form = Column(String(64))
    currency = Column(String(50))
    inv = Column(String(100))
    liac_conam = Column(DECIMAL(16, 4))


class InfoComBusAnReportShare(Base):
    __tablename__ = 'info_com_bus_an_report_share'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(BIGINT(20))
    anche_id = Column(String(300))
    alt_date = Column(DateTime)
    name = Column(String(100))
    trans_am_aft = Column(DECIMAL(16, 4))
    trans_am_bef = Column(DECIMAL(16, 4))


class InfoComBusAnReportSocial(Base):
    __tablename__ = 'info_com_bus_an_report_social'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(BIGINT(20))
    anche_id = Column(String(300))
    pension_insurance_num = Column(INTEGER(11))
    unem_insurance_num = Column(INTEGER(11))
    medical_insurance_num = Column(INTEGER(11))
    work_insurance_num = Column(INTEGER(11))
    maternity_insurance_num = Column(INTEGER(11))
    pension_insurance_amount = Column(DECIMAL(16, 4))
    unem_insurance_amount = Column(DECIMAL(16, 4))
    medical_insurance_amount = Column(DECIMAL(16, 4))
    work_insurance_amount = Column(DECIMAL(16, 4))
    maternity_insurance_amount = Column(DECIMAL(16, 4))
    pension_insurance_base = Column(DECIMAL(16, 4))
    unem_insurance_base = Column(DECIMAL(16, 4))
    medical_insurance_base = Column(DECIMAL(16, 4))
    work_insurance_base = Column(DECIMAL(16, 4))
    maternity_insurance_base = Column(DECIMAL(16, 4))
    pension_insurance_arr = Column(DECIMAL(16, 4))
    unem_insurance_arr = Column(DECIMAL(16, 4))
    medical_insurance_arr = Column(DECIMAL(16, 4))
    work_insurance_arr = Column(DECIMAL(16, 4))
    maternity_insurance_arr = Column(DECIMAL(16, 4))


class InfoComBusAnReportWeb(Base):
    __tablename__ = 'info_com_bus_an_report_web'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(BIGINT(20))
    anche_id = Column(String(300))
    web_location = Column(String(64))
    web_name = Column(String(64))
    web_type = Column(String(64))


class InfoComBusBasic(Base):
    __tablename__ = 'info_com_bus_basic'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    ent_name = Column(String(64))
    credit_code = Column(String(32))
    reg_no = Column(String(32))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime, nullable=False)
    create_time = Column(DateTime)
    modify_time = Column(DateTime)


class InfoComBusCase(Base):
    __tablename__ = 'info_com_bus_case'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    pendec_no = Column(String(255))
    illegact_type_name = Column(Text)
    pen_result_name = Column(Text)
    pen_auth_name = Column(String(64))
    pen_deciss_date = Column(DateTime)
    open_date = Column(DateTime)
    case_book = Column(Text)
    pen_type = Column(String(100))
    pen_type_cn = Column(String(100))
    pen_auth = Column(String(64))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusEntinvitem(Base):
    __tablename__ = 'info_com_bus_entinvitem'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    ent_name = Column(String(64))
    ent_id = Column(String(64))
    credit_code = Column(String(32))
    reg_no = Column(String(32))
    ent_type = Column(String(64))
    fr_name = Column(String(64))
    fr_person_id = Column(String(64))
    reg_cap = Column(DECIMAL(16, 4))
    reg_cap_cur = Column(String(50))
    es_date = Column(DateTime)
    reg_org = Column(String(64))
    ent_status = Column(String(32))
    can_date = Column(DateTime)
    rev_date = Column(DateTime)
    sub_conam = Column(DECIMAL(16, 4))
    sub_currency = Column(String(50))
    funded_ratio = Column(DECIMAL(16, 4))
    pinv_amount = Column(INTEGER(11))
    con_form = Column(String(100))
    reg_org_code = Column(String(32))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusException(Base):
    __tablename__ = 'info_com_bus_exception'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    ent_name = Column(String(64))
    reg_no = Column(String(32))
    credit_code = Column(String(64))
    result_in = Column(Text)
    date_in = Column(DateTime)
    org_name_in = Column(String(64))
    result_out = Column(Text)
    date_out = Column(DateTime)
    org_name_out = Column(String(64))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusFace(Base):
    __tablename__ = 'info_com_bus_face'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    ent_name = Column(String(64))
    ent_id = Column(String(64))
    credit_code = Column(String(32))
    reg_no = Column(String(32))
    org_code = Column(String(32))
    ent_type = Column(String(64))
    fr_name = Column(String(64))
    fr_person_id = Column(String(64))
    reg_cap = Column(DECIMAL(16, 4))
    reg_cap_cur = Column(String(50))
    rec_cap = Column(DECIMAL(16, 4))
    es_date = Column(DateTime)
    open_from = Column(DateTime)
    open_to = Column(DateTime)
    reg_org = Column(String(64))
    appr_date = Column(DateTime)
    ent_status = Column(String(32))
    can_date = Column(DateTime)
    rev_date = Column(DateTime)
    address = Column(String(100))
    province = Column(String(32))
    city = Column(String(64))
    county = Column(String(64))
    area_code = Column(String(10))
    operate_scope = Column(Text)
    anche_year = Column(String(10))
    anche_date = Column(DateTime)
    industry_phy_code = Column(String(32))
    industry_phyname = Column(String(64))
    industry_code = Column(String(32))
    industry_name = Column(String(64))
    ent_name_eng = Column(String(200))
    ori_reg_no = Column(String(32))
    email = Column(String(32))
    tel = Column(String(15))
    emp_num = Column(INTEGER(11))
    reg_org_code = Column(String(32))
    zs_ops_cope = Column(Text)
    ent_name_old = Column(String(100))
    ent_type_code = Column(String(64))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusFiliation(Base):
    __tablename__ = 'info_com_bus_filiation'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    br_name = Column(String(64))
    ent_id = Column(String(64))
    credit_code = Column(String(32))
    brreg_no = Column(String(32))
    brprincipal = Column(String(32))
    braddr = Column(String(100))
    operatescope = Column(Text)
    brnregorg = Column(String(64))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusFrinv(Base):
    __tablename__ = 'info_com_bus_frinv'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    ent_name = Column(String(64))
    ent_id = Column(String(64))
    credit_code = Column(String(32))
    reg_no = Column(String(32))
    ent_type = Column(String(64))
    fr_name = Column(String(64))
    fr_person_id = Column(String(64))
    reg_cap = Column(DECIMAL(16, 4))
    reg_cap_cur = Column(String(50))
    es_date = Column(DateTime)
    reg_org = Column(String(64))
    ent_status = Column(String(32))
    can_date = Column(DateTime)
    rev_date = Column(DateTime)
    sub_conam = Column(DECIMAL(16, 4))
    sub_currency = Column(String(50))
    funded_ratio = Column(DECIMAL(16, 4))
    reg_org_code = Column(String(50))
    con_form = Column(String(100))
    pinv_amount = Column(INTEGER(11))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusHistory(Base):
    __tablename__ = 'info_com_bus_history'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    enthis_name = Column(String(64))
    valid_date = Column(DateTime)
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusIllegal(Base):
    __tablename__ = 'info_com_bus_illegal'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    illegal_type = Column(String(100))
    illegal_result_in = Column(Text)
    illegal_date_in = Column(DateTime)
    illegal_org_name_in = Column(String(64))
    illegal_rresult_out = Column(Text)
    illegal_date_out = Column(DateTime)
    illegal_org_name_out = Column(String(64))
    illegal_symbol = Column(String(128))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusImpawnCancel(Base):
    __tablename__ = 'info_com_bus_impawn_cancel'

    id = Column(BIGINT(20), primary_key=True)
    impawn_id = Column(BIGINT(20))
    stk_pawn_date = Column(DateTime)
    stk_pawn_res = Column(Text)
    url = Column(String(100))


class InfoComBusImpawnChange(Base):
    __tablename__ = 'info_com_bus_impawn_change'

    id = Column(BIGINT(20), primary_key=True)
    impawn_id = Column(BIGINT(20))
    alt_date = Column(DateTime)
    alt_conten = Column(Text)
    url = Column(String(100))


class InfoComBusLiquidation(Base):
    __tablename__ = 'info_com_bus_liquidation'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    liq_prin_cipal = Column(String(64))
    liq_men = Column(Text)
    liq_liable = Column(String(64))
    ligst = Column(String(100))
    ligend_date = Column(DateTime)
    debt_tranee = Column(String(64))
    claim_tranee = Column(String(64))
    addr = Column(String(100))
    tel = Column(String(15))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusMortBasic(Base):
    __tablename__ = 'info_com_bus_mort_basic'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    jhi_role = Column(String(100))
    mort_gager = Column(String(64))
    mort_cer_type_name = Column(String(50))
    mort_cer_no = Column(String(32))
    mort_reg_no = Column(String(128))
    reg_date = Column(DateTime)
    reg_org = Column(String(64))
    ma_bgs_date = Column(DateTime)
    pri_cla_sec_am = Column(DECIMAL(16, 4))
    mort_status = Column(String(32))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusMortCancel(Base):
    __tablename__ = 'info_com_bus_mort_cancel'

    id = Column(BIGINT(20), primary_key=True)
    mort_id = Column(BIGINT(20))
    mort_reg_no = Column(String(128))
    can_reason = Column(String(500))
    can_date = Column(DateTime)


class InfoComBusMortChange(Base):
    __tablename__ = 'info_com_bus_mort_change'

    id = Column(BIGINT(20), primary_key=True)
    mort_id = Column(BIGINT(20))
    mort_reg_no = Column(String(128))
    ma_balt_details = Column(Text)
    ma_balt_date = Column(DateTime)


class InfoComBusMortCollateral(Base):
    __tablename__ = 'info_com_bus_mort_collateral'

    id = Column(BIGINT(20), primary_key=True)
    mort_id = Column(BIGINT(20))
    mort_reg_no = Column(String(128))
    gua_name = Column(Text)
    gua_own = Column(String(64))
    gua_des = Column(Text)
    gua_remark = Column(Text)


class InfoComBusMortCreditor(Base):
    __tablename__ = 'info_com_bus_mort_creditor'

    id = Column(BIGINT(20), primary_key=True)
    mort_id = Column(BIGINT(20))
    mort_reg_no = Column(String(128))
    pri_clasec_kind = Column(String(255))
    pri_cla_sec_am = Column(DECIMAL(16, 4))
    war_cov = Column(Text)
    pef_per_from = Column(DateTime)
    pef_per_to = Column(DateTime)
    pef_remark = Column(String(255))


class InfoComBusMortHolder(Base):
    __tablename__ = 'info_com_bus_mort_holder'

    id = Column(BIGINT(20), primary_key=True)
    mort_id = Column(BIGINT(20))
    mort_reg_no = Column(String(128))
    mort_org = Column(String(64))
    blic_type = Column(String(50))
    blic_no = Column(String(32))
    mort_loc = Column(String(100))


class InfoComBusMortRegiste(Base):
    __tablename__ = 'info_com_bus_mort_registe'

    id = Column(BIGINT(20), primary_key=True)
    mort_id = Column(BIGINT(20))
    mort_reg_no = Column(String(128))
    reg_date = Column(DateTime)
    reg_org = Column(String(64))
    pef_per_from = Column(DateTime)
    pef_per_to = Column(DateTime)
    mab_guar_amt = Column(DECIMAL(16, 4))
    mab_guar_type = Column(String(255))
    mab_guar_range = Column(Text)
    node_num = Column(String(32))
    status = Column(String(128))


class InfoComBusPunishBreak(Base):
    __tablename__ = 'info_com_bus_punish_break'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    age = Column(INTEGER(11))
    area_name = Column(String(32))
    business_entity = Column(String(64))
    card_num = Column(String(32))
    case_code = Column(String(100))
    case_state = Column(String(100))
    court_name = Column(String(64))
    disrupt_type_name = Column(Text)
    duty = Column(Text)
    exit_date = Column(DateTime)
    focus_number = Column(INTEGER(11))
    gist_id = Column(String(128))
    gist_unit = Column(String(64))
    name = Column(String(64))
    performance = Column(String(255))
    perform_part = Column(DECIMAL(16, 4))
    publish_date_clean = Column(DateTime)
    reg_date_clean = Column(DateTime)
    sex = Column(String(50))
    jhi_type = Column(String(300))
    un_perform_part = Column(DECIMAL(16, 4))
    original_place_id_card = Column(String(100))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusPunished(Base):
    __tablename__ = 'info_com_bus_punished'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    age = Column(INTEGER(11))
    area_name = Column(String(32))
    card_num = Column(String(32))
    case_code = Column(String(100))
    case_state = Column(String(100))
    court_name = Column(String(64))
    exec_money = Column(DECIMAL(16, 4))
    focus_number = Column(INTEGER(11))
    name = Column(String(64))
    reg_date = Column(DateTime)
    sex = Column(String(50))
    jhi_type = Column(String(100))
    original_place_id_card = Column(String(100))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusRelBreach(Base):
    __tablename__ = 'info_com_bus_rel_breach'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    gist_unit = Column(String(100))
    duty = Column(String(255))
    performance = Column(Text)
    perform_dpart = Column(DECIMAL(16, 4))
    un_perform_dpart = Column(DECIMAL(16, 4))
    disrupt_type_name = Column(Text)
    business_entity = Column(String(64))
    area_name = Column(String(32))
    sex = Column(String(50))
    age = Column(INTEGER(11))
    focus_number = Column(INTEGER(11))
    original_place_id_card = Column(String(100))
    exit_date = Column(DateTime)
    case_state = Column(String(100))
    jhi_type = Column(String(100))
    name = Column(String(64))
    publish_date = Column(DateTime)
    reg_date = Column(DateTime)
    card_num = Column(String(32))
    court_name = Column(String(64))
    gist_id = Column(String(128))
    case_code = Column(String(128))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusRelExcute(Base):
    __tablename__ = 'info_com_bus_rel_excute'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    court_name = Column(String(64))
    case_code = Column(String(128))
    jhi_type = Column(String(255))
    card_num = Column(String(32))
    area_name = Column(String(32))
    original_place_id_card = Column(String(100))
    age = Column(INTEGER(11))
    focus_number = Column(INTEGER(11))
    sex = Column(String(50))
    name = Column(String(64))
    case_state = Column(String(255))
    exec_money = Column(DECIMAL(16, 4))
    reg_date = Column(DateTime)
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusSenior(Base):
    __tablename__ = 'info_com_bus_senior'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    ent_name = Column(String(64))
    position = Column(String(100))
    person_id = Column(String(64))
    sex = Column(String(50))
    person_amount = Column(INTEGER(11))
    is_fr = Column(BIT(1))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusShareholder(Base):
    __tablename__ = 'info_com_bus_shareholder'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    share_holder_name = Column(String(64))
    share_id = Column(String(64))
    share_holder_type = Column(String(64))
    blic_type = Column(String(50))
    blic_no = Column(String(64))
    sub_conam = Column(DECIMAL(16, 4))
    sub_currency = Column(String(50))
    con_date = Column(DateTime)
    funded_ratio = Column(DECIMAL(16, 4))
    country = Column(String(64))
    con_form = Column(String(100))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusSharesFrost(Base):
    __tablename__ = 'info_com_bus_shares_frost'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    jhi_role = Column(String(100))
    froz_ent = Column(String(64))
    froz_cer_type_name = Column(String(50))
    froz_cer_no = Column(String(64))
    judicial_inv = Column(String(64))
    judicial_cer_type_name = Column(String(50))
    judicial_cer_no = Column(String(64))
    judicial_fro_am = Column(DECIMAL(16, 4))
    judicial_froz_state = Column(String(255))
    froz_auth = Column(String(64))
    froz_execute_item = Column(String(255))
    froz_execute_no = Column(String(128))
    froz_doc_no = Column(String(128))
    froz_from = Column(DateTime)
    froz_to = Column(DateTime)
    froz_deadline = Column(INTEGER(11))
    froz_public_date = Column(DateTime)
    keep_froz_from = Column(DateTime)
    keep_froz_to = Column(DateTime)
    keep_froz_deadline = Column(INTEGER(11))
    thaw_aut = Column(String(64))
    thaw_execute_item = Column(Text)
    thaw_execute_no = Column(String(128))
    thaw_doc_no = Column(String(128))
    thaw_date = Column(DateTime)
    thaw_public_date = Column(DateTime)
    invalid_time = Column(DateTime)
    invalid_reason = Column(Text)
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusSharesImpawn(Base):
    __tablename__ = 'info_com_bus_shares_impawn'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    imp_equity_no = Column(String(64))
    jhi_role = Column(String(64))
    pl_edge_ent = Column(String(64))
    pl_edge_ent_license_no = Column(String(64))
    imp_pled_gor = Column(String(64))
    imp_prlicense_no = Column(String(64))
    imp_am = Column(DECIMAL(16, 4))
    imp_org = Column(String(500))
    imp_org_license_no = Column(String(64))
    imp_equple_date = Column(DateTime)
    imp_exe_state = Column(String(32))
    imp_pub_date = Column(DateTime)
    url = Column(Text)
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusStock(Base):
    __tablename__ = 'info_com_bus_stock'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    stock_code = Column(String(32))
    stock_type = Column(String(100))
    stock_name = Column(String(32))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoComBusTop(Base):
    __tablename__ = 'info_com_bus_top'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    ratio_no = Column(INTEGER(11))
    share_holder_name = Column(String(64))
    share_id = Column(String(64))
    ratio = Column(DECIMAL(16, 4))
    quantity = Column(INTEGER(11))
    last_date = Column(DateTime)
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoCombusFrposition(Base):
    __tablename__ = 'info_combus_frposition'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    fr_name = Column(String(64))
    fr_person_id = Column(String(64))
    ent_name = Column(String(64))
    ent_id = Column(String(64))
    credit_code = Column(String(32))
    reg_no = Column(String(32))
    ent_type = Column(String(64))
    reg_cap = Column(DECIMAL(16, 4))
    reg_cap_cur = Column(String(50))
    es_date = Column(DateTime)
    reg_org = Column(String(64))
    ent_status = Column(String(32))
    can_date = Column(DateTime)
    rev_date = Column(DateTime)
    position = Column(String(64))
    le_rep_sign = Column(BIT(1))
    pinv_amount = Column(INTEGER(11))
    reg_org_code = Column(String(50))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoCompanyCreditCode(Base):
    __tablename__ = 'info_company_credit_code'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    ent_name = Column(String(64))
    credit_code = Column(String(32))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime, nullable=False)
    create_time = Column(DateTime)
    modify_time = Column(DateTime)


class InfoConsumptionLevel(Base):
    __tablename__ = 'info_consumption_level'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    user_name = Column(String(64))
    phone = Column(String(15))
    id_card_no = Column(String(64))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)
    near_by_3_min = Column(DECIMAL(16, 4))
    near_by_3_max = Column(DECIMAL(16, 4))
    near_by_6_min = Column(DECIMAL(16, 4))
    near_by_6_max = Column(DECIMAL(16, 4))
    near_by_12_min = Column(DECIMAL(16, 4))
    near_12_by_max = Column(DECIMAL(16, 4))
    cause = Column(String(64))
    create_time = Column(DateTime)
    modify_time = Column(DateTime)


class InfoCourt(Base):
    __tablename__ = 'info_court'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime, nullable=False)
    create_time = Column(DateTime)
    modify_time = Column(DateTime)
    unique_name = Column(String(32))
    unique_id_no = Column(String(64))
    message = Column(String(500))
    success = Column(BIT(1))
    cause = Column(String(128))
    cus_type = Column(String(50))


class InfoCourtAdministrativeViolation(Base):
    __tablename__ = 'info_court_administrative_violation'

    id = Column(BIGINT(20), primary_key=True)
    court_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    expired_at = Column(DateTime)
    channel_api_no = Column(String(20))
    id_no = Column(String(64))
    name = Column(String(32))
    objection = Column(Text)
    illegalreason = Column(Text)
    execution_result = Column(String(255))
    specific_date = Column(DateTime)
    case_no = Column(String(32))
    trial_authority = Column(String(64))
    trial_result = Column(String(255))


class InfoCourtArrearage(Base):
    __tablename__ = 'info_court_arrearage'

    id = Column(BIGINT(20), primary_key=True)
    court_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    expired_at = Column(DateTime)
    channel_api_no = Column(String(20))
    id_no = Column(String(64))
    name = Column(String(64))
    objection = Column(Text)
    default_reason = Column(Text)
    default_currency = Column(String(64))
    default_amount = Column(DECIMAL(16, 4))
    default_date = Column(DateTime)
    contract_no = Column(String(32))


class InfoCourtCriminalSuspect(Base):
    __tablename__ = 'info_court_criminal_suspect'

    id = Column(BIGINT(20), primary_key=True)
    court_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    expired_at = Column(DateTime)
    channel_api_no = Column(String(20))
    id_no = Column(String(64))
    name = Column(String(64))
    objection = Column(Text)
    trial_date = Column(DateTime)
    criminal_reason = Column(Text)
    case_no = Column(String(64))
    trial_authority = Column(String(128))
    trial_result = Column(Text)


class InfoCourtDeadbeat(Base):
    __tablename__ = 'info_court_deadbeat'

    id = Column(BIGINT(20), primary_key=True)
    court_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    expired_at = Column(DateTime)
    channel_api_no = Column(String(20))
    execute_court = Column(String(64))
    date_type = Column(String(32))
    execute_content = Column(Text)
    name = Column(String(64))
    id_no = Column(String(64))
    execute_status = Column(String(32))
    execute_case_no = Column(String(32))
    objection = Column(Text)
    execute_date = Column(DateTime)


class InfoCourtExcutePublic(Base):
    __tablename__ = 'info_court_excute_public'

    id = Column(BIGINT(20), primary_key=True)
    court_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    expired_at = Column(DateTime)
    channel_api_no = Column(String(20))
    execute_court = Column(String(64))
    execute_content = Column(Text)
    name = Column(String(64))
    id_no = Column(String(64))
    execute_status = Column(String(32))
    execute_case_no = Column(String(32))
    objection = Column(Text)
    filing_time = Column(DateTime)


class InfoCourtJudicativePape(Base):
    __tablename__ = 'info_court_judicative_pape'

    id = Column(BIGINT(20), primary_key=True)
    court_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    expired_at = Column(DateTime)
    channel_api_no = Column(String(20))
    id_no = Column(String(64))
    name = Column(String(64))
    objection = Column(Text)
    case_reason = Column(Text)
    trial_authority = Column(String(64))
    closed_time = Column(DateTime)
    case_no = Column(String(32))
    legal_status = Column(String(32))
    case_amount = Column(DECIMAL(16, 4))
    url = Column(String(255))
    oss_key = Column(String(128))


class InfoCourtLimitHignspending(Base):
    __tablename__ = 'info_court_limit_hignspending'

    id = Column(BIGINT(20), primary_key=True)
    court_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    expired_at = Column(DateTime)
    channel_api_no = Column(String(20))
    id_no = Column(String(64))
    name = Column(String(64))
    objection = Column(Text)
    execute_court = Column(String(64))
    execute_content = Column(Text)
    specific_date = Column(DateTime)
    date_type = Column(String(32))
    execute_status = Column(String(32))
    execute_case_no = Column(String(32))


class InfoCourtLimitedEntryExit(Base):
    __tablename__ = 'info_court_limited_entry_exit'

    id = Column(BIGINT(20), primary_key=True)
    court_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    expired_at = Column(DateTime)
    channel_api_no = Column(String(20))
    id_no = Column(String(64))
    name = Column(String(64))
    objection = Column(Text)
    execute_court = Column(String(32))
    execute_content = Column(Text)
    execute_no = Column(String(32))
    execute_status = Column(String(32))
    specific_date = Column(DateTime)
    date_type = Column(String(16))


class InfoCourtTaxArrear(Base):
    __tablename__ = 'info_court_tax_arrears'

    id = Column(BIGINT(20), primary_key=True)
    court_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    expired_at = Column(DateTime)
    channel_api_no = Column(String(20))
    id_no = Column(String(64))
    name = Column(String(64))
    objection = Column(Text)
    tax_authority = Column(String(64))
    taxes = Column(DECIMAL(16, 4))
    taxes_time = Column(DateTime)
    taxes_type = Column(String(32))
    tax_period = Column(String(32))


class InfoCourtTaxableAbnormalUser(Base):
    __tablename__ = 'info_court_taxable_abnormal_user'

    id = Column(BIGINT(20), primary_key=True)
    court_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    expired_at = Column(DateTime)
    channel_api_no = Column(String(20))
    id_no = Column(String(64))
    name = Column(String(64))
    objection = Column(Text)
    tax_authority = Column(String(64))
    confirm_date = Column(DateTime)


class InfoCourtTrialProces(Base):
    __tablename__ = 'info_court_trial_process'

    id = Column(BIGINT(20), primary_key=True)
    court_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    expired_at = Column(DateTime)
    channel_api_no = Column(String(20))
    id_no = Column(String(64))
    name = Column(String(64))
    objection = Column(Text)
    case_reason = Column(Text)
    trial_authority = Column(String(64))
    specific_date = Column(DateTime)
    case_no = Column(String(64))
    legal_status = Column(String(32))
    date_type = Column(String(32))


class InfoCreditBase(Base):
    __tablename__ = 'info_credit_base'

    id = Column(BIGINT(20), primary_key=True)
    risk_subject_id = Column(BIGINT(20))
    cus_name = Column(String(30), nullable=False)
    cus_code = Column(String(20), nullable=False)
    cus_type = Column(String(50))
    report_type = Column(String(50))
    report_no = Column(String(32), nullable=False, unique=True)
    report_time = Column(DateTime, nullable=False)
    oss_key = Column(String(100))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)
    create_time = Column(DateTime)
    modify_time = Column(DateTime)


class InfoCreditSimplePersonAssetDisposal(Base):
    __tablename__ = 'info_credit_simple_person_asset_disposal'

    id = Column(BIGINT(20), primary_key=True)
    report_no = Column(String(32), nullable=False)
    creditor_transfer_date = Column(DateTime)
    creditor = Column(String(255))
    target = Column(String(255))
    debt_amount = Column(DECIMAL(16, 4))
    left_amount = Column(DECIMAL(16, 4))
    last_payment_date = Column(DateTime)


class InfoCreditSimplePersonCivialJudgement(Base):
    __tablename__ = 'info_credit_simple_person_civial_judgement'

    id = Column(BIGINT(20), primary_key=True)
    report_no = Column(String(32), nullable=False)
    court = Column(String(255))
    case_no = Column(String(255))
    case_subject = Column(String(255))
    filing_time = Column(DateTime)
    target = Column(String(255))
    target_value = Column(DECIMAL(16, 4))
    settle_type = Column(String(255))
    settle_result = Column(String(255))
    effective_date = Column(DateTime)


class InfoCreditSimplePersonCompensation(Base):
    __tablename__ = 'info_credit_simple_person_compensation'

    id = Column(BIGINT(20), primary_key=True)
    report_no = Column(String(32), nullable=False)
    compensator = Column(String(255))
    last_payment_date_by_compensator = Column(DateTime)
    amount_paid_by_compensator = Column(DECIMAL(16, 4))
    last_payment_date = Column(DateTime)
    left_amount = Column(DECIMAL(16, 4))


class InfoCreditSimplePersonCreditCard(Base):
    __tablename__ = 'info_credit_simple_person_credit_card'

    id = Column(BIGINT(20), primary_key=True)
    report_no = Column(String(32), nullable=False)
    issuer = Column(String(20))
    issue_time = Column(DateTime)
    currency = Column(String(10))
    status = Column(String(10))
    close_date = Column(DateTime)
    credit_limit = Column(DECIMAL(16, 4))
    credit_used = Column(DECIMAL(16, 4))
    last_credit_time = Column(DateTime)
    overdues = Column(INTEGER(11))
    overdue_amount = Column(DECIMAL(16, 4))
    overdues_90_days = Column(INTEGER(11))
    over_draw_60_days = Column(INTEGER(11))
    over_draw_90_days = Column(INTEGER(11))
    institute_statement = Column(String(100))


class InfoCreditSimplePersonDebitCard(Base):
    __tablename__ = 'info_credit_simple_person_debit_card'

    id = Column(BIGINT(20), primary_key=True)
    report_no = Column(String(32), nullable=False)
    issuer = Column(String(20))
    issue_time = Column(DateTime)
    currency = Column(String(10))
    status = Column(String(10))
    close_date = Column(DateTime)
    credit_limit = Column(DECIMAL(16, 4))
    credit_used = Column(DECIMAL(16, 4))
    last_credit_time = Column(DateTime)
    overdues = Column(INTEGER(11))
    overdue_amount = Column(DECIMAL(16, 4))
    overdues_90_days = Column(INTEGER(11))
    over_draw_60_days = Column(INTEGER(11))
    over_draw_90_days = Column(INTEGER(11))
    institute_statement = Column(String(100))


class InfoCreditSimplePersonExecution(Base):
    __tablename__ = 'info_credit_simple_person_execution'

    id = Column(BIGINT(20), primary_key=True)
    report_no = Column(String(32), nullable=False)
    court = Column(String(255))
    case_no = Column(String(255))
    case_subject = Column(String(255))
    filing_time = Column(DateTime)
    target = Column(String(255))
    target_value = Column(DECIMAL(16, 4))
    settletime = Column(DateTime)
    settle_type = Column(String(255))
    case_status = Column(String(255))
    execution_target = Column(String(255))
    execution_target_value = Column(DECIMAL(16, 4))


class InfoCreditSimplePersonGuarantee(Base):
    __tablename__ = 'info_credit_simple_person_guarantee'

    id = Column(BIGINT(20), primary_key=True)
    report_no = Column(String(32), nullable=False)
    debtor = Column(String(255))
    debtor_id_type = Column(String(255))
    debtor_id = Column(String(255))
    guarantee_date = Column(DateTime)
    creditor = Column(String(255))
    target = Column(String(255))
    loan_amount = Column(DECIMAL(16, 4))
    guarantee_amount = Column(DECIMAL(16, 4))
    loan_balance = Column(DECIMAL(16, 4))
    balance_date = Column(DateTime)


class InfoCreditSimplePersonHouseLoan(Base):
    __tablename__ = 'info_credit_simple_person_house_loan'

    id = Column(BIGINT(20), primary_key=True)
    report_no = Column(String(32), nullable=False)
    issuer = Column(String(255))
    issue_date = Column(DateTime)
    amount = Column(DECIMAL(16, 4))
    currency = Column(String(10))
    jhi_type = Column(String(255))
    status = Column(String(10))
    close_date = Column(DateTime)
    due_date = Column(DateTime)
    balance = Column(DECIMAL(16, 4))
    balance_date = Column(DateTime)
    overdues = Column(INTEGER(11))
    overdue_amount = Column(DECIMAL(16, 4))
    overdues_90_days = Column(INTEGER(11))


class InfoCreditSimplePersonInstituteQuery(Base):
    __tablename__ = 'info_credit_simple_person_institute_query'

    id = Column(BIGINT(20), primary_key=True)
    report_no = Column(String(32), nullable=False)
    reason = Column(String(255))
    query_date = Column(DateTime)
    queried_by = Column(String(255))


class InfoCreditSimplePersonOtherLoan(Base):
    __tablename__ = 'info_credit_simple_person_other_loan'

    id = Column(BIGINT(20), primary_key=True)
    report_no = Column(String(32), nullable=False)
    issuer = Column(String(255))
    issue_date = Column(DateTime)
    amount = Column(DECIMAL(16, 4))
    currency = Column(String(10))
    jhi_type = Column(String(20))
    status = Column(String(10))
    close_date = Column(DateTime)
    due_date = Column(DateTime)
    balance = Column(DECIMAL(16, 4))
    balance_date = Column(DateTime)
    overdues = Column(INTEGER(11))
    overdue_amount = Column(DECIMAL(16, 4))
    overdues_90_days = Column(INTEGER(11))


class InfoCreditSimplePersonPenalty(Base):
    __tablename__ = 'info_credit_simple_person_penalty'

    id = Column(BIGINT(20), primary_key=True)
    report_no = Column(String(32), nullable=False)
    institute = Column(String(255))
    record_no = Column(String(255))
    penalty_content = Column(String(255))
    penalty_amount = Column(DECIMAL(16, 4))
    effective_date = Column(DateTime)
    due_date = Column(DateTime)
    review = Column(BIT(1))
    review_result = Column(String(255))


class InfoCreditSimplePersonPersonalQuery(Base):
    __tablename__ = 'info_credit_simple_person_personal_query'

    id = Column(BIGINT(20), primary_key=True)
    report_no = Column(String(32), nullable=False)
    reason = Column(String(255))
    query_date = Column(DateTime)
    queried_by = Column(String(255))


class InfoCreditSimplePersonSummary(Base):
    __tablename__ = 'info_credit_simple_person_summary'

    id = Column(BIGINT(20), primary_key=True)
    report_no = Column(String(32), nullable=False, unique=True)
    marriage = Column(String(255))
    query_time = Column(DateTime)
    compensations = Column(INTEGER(11))
    asset_disposals = Column(INTEGER(11))
    credit_card_accounts_overdued = Column(INTEGER(11))
    credit_card_total_accounts = Column(INTEGER(11))
    credit_card_accounts_overdued_90_days = Column(INTEGER(11))
    credit_card_guarantees = Column(INTEGER(11))
    credit_card_active_accounts = Column(INTEGER(11))
    house_loan_accounts_overdued = Column(INTEGER(11))
    house_loan_total_accounts = Column(INTEGER(11))
    house_loan_accounts_overdued_90_days = Column(INTEGER(11))
    house_loan_guarantees = Column(INTEGER(11))
    house_loan_active_accounts = Column(INTEGER(11))
    other_loan_accounts_overdued = Column(INTEGER(11))
    other_loan_total_accounts = Column(INTEGER(11))
    other_loan_accounts_overdued_90_days = Column(INTEGER(11))
    other_loan_guarantees = Column(INTEGER(11))
    other_loan_active_accounts = Column(INTEGER(11))


class InfoCreditSimplePersonTelDefault(Base):
    __tablename__ = 'info_credit_simple_person_tel_default'

    id = Column(BIGINT(20), primary_key=True)
    report_no = Column(String(32), nullable=False)
    carrier = Column(String(255))
    service_type = Column(String(255))
    start_time = Column(DateTime)
    default_amount = Column(DECIMAL(16, 4))
    accounting_time = Column(DateTime)


class InfoCreditSimplePersonUnpaidTax(Base):
    __tablename__ = 'info_credit_simple_person_unpaid_tax'

    id = Column(BIGINT(20), primary_key=True)
    report_no = Column(String(32), nullable=False)
    authority = Column(String(255))
    jhi_time = Column(DateTime)
    amount = Column(DECIMAL(16, 4))
    tax_payer_id = Column(String(255))


class InfoCriminalCase(Base):
    __tablename__ = 'info_criminal_case'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    user_name = Column(String(64))
    id_card_no = Column(String(64))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime, nullable=False)
    valid_result = Column(BIT(1))
    status = Column(String(50))
    crime_count = Column(String(32))
    crime_type = Column(String(128))
    case_period = Column(String(32))
    cause = Column(String(64))
    create_time = Column(DateTime)
    modify_time = Column(DateTime)
    status_code = Column(INTEGER(11))
    status_msg = Column(String(64))
    score = Column(DECIMAL(16, 4))


class InfoEntCertification(Base):
    __tablename__ = 'info_ent_certification'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    ent_name = Column(String(64))
    reg_code = Column(String(32))
    credit_code = Column(String(32))
    fr_name = Column(String(64))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime, nullable=False)
    cause = Column(String(64))
    create_time = Column(DateTime)
    modify_time = Column(DateTime)


class InfoEnterpriseIndustry(Base):
    __tablename__ = 'info_enterprise_industry'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    user_name = Column(String(64))
    id_card_no = Column(String(64))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime, nullable=False)
    company_name = Column(String(64))
    industry_co = Column(String(128))
    industry_phy = Column(String(128))
    industry_sec = Column(String(128))
    industry_trd = Column(String(128))
    result_code = Column(String(8))
    result_desc = Column(String(64))
    create_time = Column(DateTime)


class InfoFraudVerification(Base):
    __tablename__ = 'info_fraud_verification'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    create_time = Column(DateTime)
    modify_time = Column(DateTime)
    expired_at = Column(DateTime, nullable=False)
    user_name = Column(String(64))
    id_card_no = Column(String(64))
    phone = Column(String(15))
    final_decision = Column(String(64))
    final_score = Column(String(64))
    id_card_address = Column(String(64))
    mobile_address = Column(String(64))
    success = Column(BIT(1))
    cause = Column(String(64))


class InfoFraudVerificationItem(Base):
    __tablename__ = 'info_fraud_verification_item'

    id = Column(BIGINT(20), primary_key=True)
    fraud_verification_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    risk_subject_id = Column(BIGINT(20))
    expired_at = Column(DateTime)
    item_id = Column(String(64))
    item_name = Column(String(64))
    risk_level = Column(String(64))
    item_group = Column(String(64))
    item_detail = Column(LONGTEXT)


class InfoLoanStat(Base):
    __tablename__ = 'info_loan_stats'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    user_name = Column(String(64))
    phone = Column(String(15))
    id_card_no = Column(String(64))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)
    status = Column(String(50))
    channel_score = Column(INTEGER(11))
    channel_score_desc = Column(String(255))
    fail_rate = Column(String(16))
    cause = Column(String(64))
    create_time = Column(DateTime)
    modify_time = Column(DateTime)


class InfoLoanStatsFund(Base):
    __tablename__ = 'info_loan_stats_fund'

    id = Column(BIGINT(20), primary_key=True)
    loan_stats_id = Column(BIGINT(20))
    recent_months = Column(String(50))
    card_id = Column(String(32))
    fund_open_6_m_have = Column(String(128))
    fund_open_6_m_count = Column(INTEGER(11))
    fund_pur = Column(INTEGER(11))
    fund_pur_eqy = Column(INTEGER(11))
    fund_pur_moy = Column(INTEGER(11))
    fund_pur_bond = Column(INTEGER(11))
    trans_count_pur = Column(INTEGER(11))
    success_count_pur = Column(INTEGER(11))
    amount_success_pur = Column(DECIMAL(16, 4))
    amount_max_pur = Column(DECIMAL(16, 4))
    amount_avg_pur = Column(DECIMAL(16, 4))
    fund_api = Column(INTEGER(11))
    fund_api_eqy = Column(INTEGER(11))
    fund_api_moy = Column(INTEGER(11))
    fund_api_bond = Column(INTEGER(11))
    trans_count_api = Column(INTEGER(11))
    success_count_api = Column(INTEGER(11))
    amount_success_api = Column(DECIMAL(16, 4))
    amount_max_api = Column(DECIMAL(16, 4))
    amount_avg_api = Column(DECIMAL(16, 4))
    fund_div = Column(INTEGER(11))
    fund_div_eqy = Column(INTEGER(11))
    fund_div_moy = Column(INTEGER(11))
    fund_div_bond = Column(INTEGER(11))
    trans_count_div = Column(INTEGER(11))
    success_count_div = Column(INTEGER(11))
    amount_success_div = Column(DECIMAL(16, 4))
    amount_max_div = Column(DECIMAL(16, 4))
    amount_avg_div = Column(DECIMAL(16, 4))
    fund_redm = Column(INTEGER(11))
    fund_redm_eqy = Column(INTEGER(11))
    fund_redm_moy = Column(INTEGER(11))
    fund_redm_bond = Column(INTEGER(11))
    trans_count_redm = Column(INTEGER(11))
    success_count_redm = Column(INTEGER(11))
    amount_success_redm = Column(DECIMAL(16, 4))
    amount_max_redm = Column(DECIMAL(16, 4))
    amount_avg_redm = Column(DECIMAL(16, 4))


class InfoLoanStatsPay(Base):
    __tablename__ = 'info_loan_stats_pay'

    id = Column(BIGINT(20), primary_key=True)
    loan_stats_id = Column(BIGINT(20))
    recent_months = Column(String(50))
    card_id = Column(String(32))
    trans_last_date = Column(DateTime)
    trans_first_date = Column(DateTime)
    latest_org_count = Column(INTEGER(11))
    latest_fail_org_count = Column(INTEGER(11))
    latest_trans_count = Column(INTEGER(11))
    latest_fail_trans_count = Column(INTEGER(11))
    low_balance_fail_count = Column(INTEGER(11))
    success_pay_total_amount = Column(DECIMAL(16, 4))
    success_pay_max_amount = Column(DECIMAL(16, 4))
    success_pay_avg_amount = Column(DECIMAL(16, 4))
    latest_other_pay_org_count = Column(INTEGER(11))
    success_other_pay_total_amount = Column(DECIMAL(16, 4))
    success_other_pay_max_amount = Column(DECIMAL(16, 4))
    success_other_pay_avg_amount = Column(DECIMAL(16, 4))


class InfoMobileAttr(Base):
    __tablename__ = 'info_mobile_attr'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    phone = Column(String(15))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime, nullable=False)
    province = Column(String(16))
    province_code = Column(String(6))
    city = Column(String(16))
    city_code = Column(String(6))
    corp_name = Column(String(16))
    card_type = Column(String(16))
    cause = Column(String(64))
    create_time = Column(DateTime)
    modify_time = Column(DateTime)


class InfoOnLineDuration(Base):
    __tablename__ = 'info_on_line_duration'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    user_name = Column(String(64))
    phone = Column(String(15))
    id_card_no = Column(String(64))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime, nullable=False)
    on_line_days = Column(BIGINT(20))
    cause = Column(String(64))
    create_time = Column(DateTime)
    modify_time = Column(DateTime)


class InfoOnLineState(Base):
    __tablename__ = 'info_on_line_state'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    user_name = Column(String(64))
    phone = Column(String(15))
    id_card_no = Column(String(64))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime, nullable=False)
    mobile_state = Column(String(50))
    cause = Column(String(64))
    create_time = Column(DateTime)
    modify_time = Column(DateTime)


class InfoOtherLoanSummary(Base):
    __tablename__ = 'info_other_loan_summary'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    user_name = Column(String(64))
    phone = Column(String(15))
    id_card_no = Column(String(64))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)
    seq_no = Column(String(64))
    name = Column(String(64))
    mobile_no = Column(INTEGER(11))
    id_no = Column(String(64))
    id_type = Column(INTEGER(11))
    detail_info_data = Column(LONGTEXT)
    org_number = Column(INTEGER(11))
    last_15_d_org_num = Column(INTEGER(11))
    last_1_m_org_num = Column(INTEGER(11))
    last_3_m_org_num = Column(INTEGER(11))
    last_6_m_org_num = Column(INTEGER(11))
    last_9_m_org_num = Column(INTEGER(11))
    last_12_m_org_num = Column(INTEGER(11))
    baks_num = Column(INTEGER(11))
    last_15_d_baks_num = Column(INTEGER(11))
    last_1_m_baks_num = Column(INTEGER(11))
    last_3_m_baks_num = Column(INTEGER(11))
    last_6_m_baks_num = Column(INTEGER(11))
    last_9_m_baks_num = Column(INTEGER(11))
    last_12_m_baks_num = Column(INTEGER(11))
    htls_num = Column(INTEGER(11))
    last_15_d_htls_num = Column(INTEGER(11))
    last_1_m_htls_num = Column(INTEGER(11))
    last_3_m_htls_num = Column(INTEGER(11))
    last_6_m_htls_num = Column(INTEGER(11))
    last_9_m_htls_num = Column(INTEGER(11))
    last_12_m_htls_num = Column(INTEGER(11))
    cnss_num = Column(INTEGER(11))
    last_15_d_cnss_num = Column(INTEGER(11))
    last_1_m_cnss_num = Column(INTEGER(11))
    last_3_m_cnss_num = Column(INTEGER(11))
    last_6_m_cnss_num = Column(INTEGER(11))
    last_9_m_cnss_num = Column(INTEGER(11))
    last_12_m_cnss_num = Column(INTEGER(11))
    p_2_ps_num = Column(INTEGER(11))
    last_15_d_p_2_ps_num = Column(INTEGER(11))
    last_1_m_p_2_ps_num = Column(INTEGER(11))
    last_3_m_p_2_ps_num = Column(INTEGER(11))
    last_6_m_p_2_ps_num = Column(INTEGER(11))
    last_9_m_p_2_ps_num = Column(INTEGER(11))
    last_12_m_p_2_ps_num = Column(INTEGER(11))
    scale_1_s_num = Column(INTEGER(11))
    last_15_d_scale_1_num = Column(INTEGER(11))
    last_1_m_scale_1_num = Column(INTEGER(11))
    last_3_m_scale_1_num = Column(INTEGER(11))
    last_6_m_scale_1_num = Column(INTEGER(11))
    last_9_m_scale_1_num = Column(INTEGER(11))
    last_12_m_scale_1_num = Column(INTEGER(11))
    scale_2_s_num = Column(INTEGER(11))
    last_15_d_scale_2_num = Column(INTEGER(11))
    last_1_m_scale_2_num = Column(INTEGER(11))
    last_3_m_scale_2_num = Column(INTEGER(11))
    last_6_m_scale_2_num = Column(INTEGER(11))
    last_9_m_scale_2_num = Column(INTEGER(11))
    last_12_m_scale_2_num = Column(INTEGER(11))
    scale_3_s_num = Column(INTEGER(11))
    last_15_d_scale_3_num = Column(INTEGER(11))
    last_1_m_scale_3_num = Column(INTEGER(11))
    last_3_m_scale_3_num = Column(INTEGER(11))
    last_6_m_scale_3_num = Column(INTEGER(11))
    last_9_m_scale_3_num = Column(INTEGER(11))
    last_12_m_scale_3_num = Column(INTEGER(11))
    scale_4_s_num = Column(INTEGER(11))
    last_15_d_scale_4_num = Column(INTEGER(11))
    last_1_m_scale_4_num = Column(INTEGER(11))
    last_3_m_scale_4_num = Column(INTEGER(11))
    last_6_m_scale_4_num = Column(INTEGER(11))
    last_9_m_scale_4_num = Column(INTEGER(11))
    last_12_m_scale_4_num = Column(INTEGER(11))
    all_months_org_query_num = Column(INTEGER(11))
    half_months_org_query_num = Column(INTEGER(11))
    one_months_org_query_num = Column(INTEGER(11))
    three_months_org_query_num = Column(INTEGER(11))
    six_months_org_query_num = Column(INTEGER(11))
    nine_months_org_query_num = Column(INTEGER(11))
    twelve_months_org_query_num = Column(INTEGER(11))
    all_months_mobile_num = Column(INTEGER(11))
    last_15_d_mobile_num = Column(INTEGER(11))
    last_1_m_mobile_num = Column(INTEGER(11))
    last_3_m_mobile_num = Column(INTEGER(11))
    last_6_m_mobile_num = Column(INTEGER(11))
    last_9_m_mobile_num = Column(INTEGER(11))
    last_12_m_mobile_num = Column(INTEGER(11))
    mobile_pop = Column(INTEGER(11))
    last_15_d_mobile_pop = Column(INTEGER(11))
    last_1_m_mobile_pop = Column(INTEGER(11))
    last_3_m_mobile_pop = Column(INTEGER(11))
    last_6_m_mobile_pop = Column(INTEGER(11))
    last_9_m_mobile_pop = Column(INTEGER(11))
    last_12_m_mobile_pop = Column(INTEGER(11))
    er_code = Column(String(32))
    er_msg = Column(String(64))
    create_time = Column(DateTime)
    modify_time = Column(DateTime)


class InfoPerBusBasic(Base):
    __tablename__ = 'info_per_bus_basic'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    ent_name = Column(String(64))
    name = Column(String(64))
    id_card_no = Column(String(64))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime, nullable=False)
    create_time = Column(DateTime)
    modify_time = Column(DateTime)


class InfoPerBusExecuted(Base):
    __tablename__ = 'info_per_bus_executed'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    age = Column(INTEGER(11))
    area = Column(String(64))
    card_address = Column(String(100))
    id_card_no = Column(String(64))
    case_code = Column(String(128))
    case_state = Column(String(128))
    court_name = Column(String(64))
    execute_money = Column(DECIMAL(16, 4))
    execute_name = Column(String(64))
    focus_number = Column(INTEGER(11))
    gender = Column(String(50))
    register_date = Column(DateTime)
    execute_type = Column(String(64))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoPerBusFaith(Base):
    __tablename__ = 'info_per_bus_faith'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    age = Column(INTEGER(11))
    area = Column(String(20))
    business_entity = Column(String(64))
    card_address = Column(String(100))
    id_card_no = Column(String(64))
    case_code = Column(String(128))
    court_name = Column(String(64))
    disrupt_type_name = Column(Text)
    execute_basis_depart = Column(String(100))
    execute_basis_number = Column(String(128))
    execute_name = Column(String(64))
    execute_type = Column(String(128))
    exit_date = Column(DateTime)
    focus_number = Column(INTEGER(11))
    gender = Column(String(50))
    legal_duty = Column(Text)
    lr_name = Column(String(64))
    perform_ance = Column(String(128))
    performed_report = Column(String(128))
    publish_date = Column(DateTime)
    register_date = Column(DateTime)
    unperformed_report = Column(String(255))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoPerBusLegal(Base):
    __tablename__ = 'info_per_bus_legal'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    ry_name = Column(String(64))
    ent_name = Column(String(64))
    reg_no = Column(String(32))
    credit_code = Column(String(32))
    ent_type = Column(String(64))
    reg_cap = Column(DECIMAL(16, 4))
    reg_cap_cur = Column(String(50))
    ent_status = Column(String(64))
    jhi_date = Column(DateTime)
    industry_phy_name = Column(String(64))
    palgorithmid = Column(String(128))
    province = Column(String(32))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoPerBusSanction(Base):
    __tablename__ = 'info_per_bus_sanction'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    credit_code = Column(String(32))
    cart_no = Column(String(32))
    case_reason = Column(Text)
    case_result = Column(Text)
    case_time = Column(DateTime)
    case_type = Column(String(64))
    case_value = Column(String(64))
    execute_sort = Column(String(64))
    illegal_fact = Column(Text)
    name = Column(String(64))
    penal_amount = Column(DECIMAL(16, 4))
    penal_basis = Column(String(255))
    penal_decision = Column(String(255))
    penal_decision_date = Column(DateTime)
    penal_decision_org = Column(String(64))
    penal_execution = Column(String(255))
    penal_result = Column(Text)
    penal_type = Column(String(128))
    pen_auth = Column(String(64))
    pen_content = Column(Text)
    pen_type_cn = Column(String(128))
    public_date = Column(DateTime)
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoPerBusSenior(Base):
    __tablename__ = 'info_per_bus_senior'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    ry_name = Column(String(64))
    ent_name = Column(String(64))
    reg_no = Column(String(32))
    credit_code = Column(String(32))
    ent_type = Column(String(64))
    reg_cap = Column(DECIMAL(16, 4))
    reg_cap_cur = Column(String(50))
    ent_status = Column(String(64))
    position = Column(String(32))
    jhi_date = Column(DateTime)
    industry_phy_name = Column(String(64))
    palgorithmid = Column(String(128))
    province = Column(String(32))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoPerBusShareholder(Base):
    __tablename__ = 'info_per_bus_shareholder'

    id = Column(BIGINT(20), primary_key=True)
    basic_id = Column(BIGINT(20))
    ry_name = Column(String(64))
    ent_name = Column(String(64))
    reg_no = Column(String(32))
    credit_code = Column(String(32))
    ent_type = Column(String(64))
    reg_cap = Column(DECIMAL(16, 4))
    reg_cap_cur = Column(String(50))
    sub_conam = Column(DECIMAL(16, 4))
    sub_currency = Column(String(50))
    ent_status = Column(String(64))
    conform = Column(String(128))
    funded_ratio = Column(DECIMAL(16, 4))
    jhi_date = Column(DateTime)
    industry_phy_name = Column(String(64))
    palgorithmid = Column(String(128))
    province = Column(String(32))
    risk_subject_id = Column(BIGINT(20))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)


class InfoRiskAntiFraud(Base):
    __tablename__ = 'info_risk_anti_fraud'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    user_name = Column(String(64))
    id_card_no = Column(String(64))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime, nullable=False)
    phone = Column(String(15))
    ip = Column(String(24))
    match_force = Column(BIT(1))
    match_dns = Column(BIT(1))
    match_mail_service = Column(BIT(1))
    match_seo = Column(BIT(1))
    match_organization = Column(BIT(1))
    match_crawler = Column(BIT(1))
    match_proxy = Column(BIT(1))
    match_ip_blacklist = Column(BIT(1))
    match_web_service = Column(BIT(1))
    match_vpn = Column(BIT(1))
    ip_risk_score = Column(INTEGER(11))
    ip_risk_date = Column(DateTime)
    match_blacklist = Column(BIT(1))
    match_crank_call = Column(BIT(1))
    match_fraud = Column(BIT(1))
    match_empty_number = Column(BIT(1))
    match_verification_mobile = Column(BIT(1))
    match_small_no = Column(BIT(1))
    match_sz_no = Column(BIT(1))
    phone_risk_date = Column(DateTime)
    ip_risk_desc = Column(String(255))
    phone_risk_desc = Column(String(255))
    bad_info_desc = Column(String(8))
    bad_info_time = Column(DateTime)
    result_code = Column(String(8))
    result_desc = Column(String(64))
    create_time = Column(DateTime)


class InfoRiskOtherLoan(Base):
    __tablename__ = 'info_risk_other_loan'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    user_name = Column(String(64))
    id_card_no = Column(String(64))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime, nullable=False)
    other_loan_id = Column(BIGINT(20))
    create_time = Column(DateTime)


class InfoRiskOtherLoanRecord(Base):
    __tablename__ = 'info_risk_other_loan_record'

    id = Column(BIGINT(20), primary_key=True)
    other_loan_id = Column(BIGINT(20))
    reason_code = Column(String(8))
    industry = Column(String(50))
    amount = Column(INTEGER(11))
    bank_amount = Column(INTEGER(11))
    consumer_finance_amount = Column(INTEGER(11))
    p_2_p_amount = Column(INTEGER(11))
    query_amount = Column(INTEGER(11))
    query_amount_three_month = Column(INTEGER(11))
    query_amount_six_month = Column(INTEGER(11))
    data_build_time = Column(DateTime)
    result_code = Column(String(8))
    result_desc = Column(String(64))
    create_time = Column(DateTime)


class InfoRiskOverdue(Base):
    __tablename__ = 'info_risk_overdue'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    user_name = Column(String(64))
    id_card_no = Column(String(64))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime, nullable=False)
    risk_overdue_id = Column(BIGINT(20))
    create_time = Column(DateTime)


class InfoRiskOverdueRecord(Base):
    __tablename__ = 'info_risk_overdue_record'

    id = Column(BIGINT(20), primary_key=True)
    risk_overdue_id = Column(BIGINT(20))
    source_type = Column(String(50))
    risk_score = Column(String(8))
    risk_mark = Column(String(50))
    data_build_time = Column(DateTime)
    data_status = Column(String(50))
    result_code = Column(String(8))
    result_desc = Column(String(64))


class InfoRiskScore(Base):
    __tablename__ = 'info_risk_score'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    user_name = Column(String(64))
    id_card_no = Column(String(64))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime, nullable=False)
    phone = Column(String(15))
    bank_card_no = Column(String(8))
    scorecard_type = Column(String(2))
    score = Column(INTEGER(11))
    result_code = Column(String(8))
    result_desc = Column(String(64))
    create_time = Column(DateTime)


class InfoSm(Base):
    __tablename__ = 'info_sms'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    user_name = Column(String(64))
    id_card_no = Column(String(64))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)
    phone = Column(String(15))
    province = Column(String(32))
    city = Column(String(32))
    result_code = Column(String(8))
    sms_id = Column(BIGINT(20))
    create_time = Column(DateTime)


class InfoSmsDebt(Base):
    __tablename__ = 'info_sms_debt'

    id = Column(BIGINT(20), primary_key=True)
    sms_id = Column(BIGINT(20))
    time_interval = Column(String(32))
    platform_code = Column(String(32))
    debt_money = Column(String(32))


class InfoSmsLoan(Base):
    __tablename__ = 'info_sms_loan'

    id = Column(BIGINT(20), primary_key=True)
    sms_id = Column(BIGINT(20))
    time_interval = Column(String(32))
    platform_type = Column(String(255))
    platform_code = Column(String(32))
    loan_time = Column(DateTime)
    loan_amount = Column(String(32))


class InfoSmsLoanApply(Base):
    __tablename__ = 'info_sms_loan_apply'

    id = Column(BIGINT(20), primary_key=True)
    sms_id = Column(BIGINT(20))
    time_interval = Column(String(32))
    platform_type = Column(String(255))
    platform_code = Column(String(32))
    apply_time = Column(DateTime)
    apply_amount = Column(String(32))
    apply_result = Column(String(32))


class InfoSmsLoanPlatform(Base):
    __tablename__ = 'info_sms_loan_platform'

    id = Column(BIGINT(20), primary_key=True)
    sms_id = Column(BIGINT(20))
    time_interval = Column(String(32))
    platform_type = Column(String(255))
    platform_code = Column(String(32))
    register_time = Column(DateTime)


class InfoSmsLoanReject(Base):
    __tablename__ = 'info_sms_loan_reject'

    id = Column(BIGINT(20), primary_key=True)
    sms_id = Column(BIGINT(20))
    time_interval = Column(String(32))
    platform_type = Column(String(255))
    platform_code = Column(String(32))
    reject_time = Column(DateTime)


class InfoSmsOverduePlatform(Base):
    __tablename__ = 'info_sms_overdue_platform'

    id = Column(BIGINT(20), primary_key=True)
    sms_id = Column(BIGINT(20))
    time_interval = Column(String(32))
    platform_code = Column(String(32))
    overdue_count = Column(INTEGER(11))
    overdue_money = Column(String(32))
    overdue_time = Column(DateTime)


class InfoSocial(Base):
    __tablename__ = 'info_social'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    user_name = Column(String(64))
    id_card_no = Column(String(64))
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime, nullable=False)
    phone = Column(String(15))
    phone_operator = Column(String(16))
    idcard_no_valid = Column(BIT(1))
    age = Column(INTEGER(11))
    gender = Column(String(2))
    phone_province = Column(String(32))
    phone_city = Column(String(32))
    province = Column(String(32))
    city = Column(String(32))
    region = Column(String(32))
    update_time = Column(DateTime)
    searched_organization = Column(INTEGER(11))
    result_code = Column(String(32))
    result_msg = Column(String(32))
    create_time = Column(DateTime)
    social_id = Column(BIGINT(20))


class InfoSocialBlacklist(Base):
    __tablename__ = 'info_social_blacklist'

    id = Column(BIGINT(20), primary_key=True)
    social_id = Column(BIGINT(20))
    blacklist_name_with_phone = Column(BIT(1))
    blacklist_name_with_idcard = Column(BIT(1))
    blacklist_category = Column(String(128))
    blacklist_detail_id = Column(BIGINT(20))
    blacklist_update_time_name_phone = Column(String(32))
    blacklist_update_time_name_idcard = Column(String(32))


class InfoSocialBlacklistChild(Base):
    __tablename__ = 'info_social_blacklist_child'

    id = Column(BIGINT(20), primary_key=True)
    blacklist_detail_id = Column(BIGINT(20))
    detail_name = Column(String(128))
    detail_value = Column(String(128))


class InfoSocialGray(Base):
    __tablename__ = 'info_social_gray'

    id = Column(BIGINT(20), primary_key=True)
    social_id = Column(BIGINT(20))
    phone = Column(String(15))
    phone_gray_score = Column(DECIMAL(16, 4))
    contacts_class_1_blacklist_cnt = Column(INTEGER(11))
    contacts_class_2_blacklist_cnt = Column(INTEGER(11))
    contacts_router_cnt = Column(INTEGER(11))
    contacts_router_ratio = Column(DECIMAL(16, 4))
    contacts_class_1_cnt = Column(INTEGER(11))


class InfoSocialSearchedHistory(Base):
    __tablename__ = 'info_social_searched_history'

    id = Column(BIGINT(20), primary_key=True)
    social_id = Column(BIGINT(20))
    searched_org = Column(String(16))
    searched_date = Column(DateTime)
    org_self = Column(BIT(1))


class InfoSocialSuspicion(Base):
    __tablename__ = 'info_social_suspicion'

    id = Column(BIGINT(20), primary_key=True)
    social_id = Column(BIGINT(20))
    suspicion_type = Column(String(255))
    suspicion = Column(String(32))
    suspicion_time = Column(DateTime)


class InfoSocialSuspicionWithPhone(Base):
    __tablename__ = 'info_social_suspicion_with_phone'

    id = Column(BIGINT(20), primary_key=True)
    social_id = Column(BIGINT(20))
    phone_province = Column(String(16))
    phone_operator = Column(String(16))
    update_time = Column(DateTime)
    phone = Column(String(255))
    phone_city = Column(String(16))


class InfoSocialUserRegister(Base):
    __tablename__ = 'info_social_user_register'

    id = Column(BIGINT(20), primary_key=True)
    social_id = Column(BIGINT(20))
    register_count = Column(INTEGER(11))
    phone = Column(String(15))
    register_org = Column(String(32))
    count = Column(INTEGER(11))
    jhi_label = Column(String(32))


class JhiPersistentAuditEvent(Base):
    __tablename__ = 'jhi_persistent_audit_event'
    __table_args__ = (
        Index('idx_persistent_audit_event', 'principal', 'event_date'),
    )

    event_id = Column(BIGINT(20), primary_key=True)
    principal = Column(String(50), nullable=False)
    event_date = Column(TIMESTAMP)
    event_type = Column(String(255))


class PcreditAccSpeculate(Base):
    __tablename__ = 'pcredit_acc_speculate'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    record_id = Column(BIGINT(20))
    speculate_record_id = Column(BIGINT(20))
    report_time = Column(DateTime)
    loan_repay_type = Column(String(32))
    nominal_interest_rate = Column(DECIMAL(16, 4))
    real_interest_rate = Column(DECIMAL(16, 4))
    repay_month = Column(String(32))
    repay_amount = Column(DECIMAL(16, 4))
    loan_balance = Column(DECIMAL(16, 4))
    account_status = Column(String(20))
    settled = Column(BIT(1))
    create_time = Column(DateTime)


class PcreditAccountCollection(Base):
    __tablename__ = 'pcredit_account_collection'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    account_type = Column(INTEGER(11))
    total_count = Column(INTEGER(11))
    uncleared_count = Column(INTEGER(11))
    overdue_count = Column(INTEGER(11))
    overdue_count_day_90 = Column(INTEGER(11))
    assure_count = Column(INTEGER(11))


class PcreditAssetsManage(Base):
    __tablename__ = 'pcredit_assets_manage'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    no = Column(INTEGER(11))
    biz_type = Column(String(16))
    asset_manage_company = Column(String(20))
    debt_date = Column(Date)
    debt_amount = Column(DECIMAL(16, 4))
    debt_status = Column(String(32))
    expiry_date = Column(DateTime)
    account_status = Column(String(32))
    lately_repay_date = Column(Date)
    amount = Column(DECIMAL(16, 4))


class PcreditBizInfo(Base):
    __tablename__ = 'pcredit_biz_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    biz_type = Column(String(16))
    biz_sub_type = Column(String(32))
    biz_counts = Column(INTEGER(11))
    biz_first_month = Column(String(16))


class PcreditCarTradeRecord(Base):
    __tablename__ = 'pcredit_car_trade_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    car_no = Column(String(32))
    engine_no = Column(String(128))
    car_brand = Column(String(64))
    car_type = Column(String(32))
    use_type = Column(String(32))
    car_status = Column(String(32))
    mortgage_tag = Column(String(32))
    info_update_date = Column(DateTime)


class PcreditCardInstitution(Base):
    __tablename__ = 'pcredit_card_institution'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    org_desc = Column(String(128))
    org_desc_add_date = Column(DateTime)
    jhi_declare = Column(String(128))
    declare_add_date = Column(DateTime)
    remark = Column(String(128))
    remark_add_date = Column(DateTime)


class PcreditCertInfo(Base):
    __tablename__ = 'pcredit_cert_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    cert_type = Column(String(32))
    cert_no = Column(String(32))


class PcreditCivilJudgmentsRecord(Base):
    __tablename__ = 'pcredit_civil_judgments_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    court_name = Column(String(128))
    cause = Column(LONGTEXT)
    register_date = Column(DateTime)
    result_type = Column(String(32))
    result = Column(String(256))
    effective_date = Column(DateTime)
    litigious = Column(String(64))
    litigious_amt = Column(DECIMAL(16, 4))


class PcreditCreditCard(Base):
    __tablename__ = 'pcredit_credit_card'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    jhi_type = Column(INTEGER(11))
    info = Column(String(200))
    start_date = Column(Date)
    bank_name = Column(String(32))
    card_type = Column(INTEGER(11))
    account_type = Column(INTEGER(11))
    credit_line = Column(DECIMAL(16, 4))
    used_line = Column(DECIMAL(16, 4))
    over_line = Column(DECIMAL(16, 4))
    remark = Column(String(200))
    overdue_month_year_5 = Column(INTEGER(11))
    overdue_month_day_90 = Column(INTEGER(11))
    overdraft_month_year_5 = Column(INTEGER(11))
    overdraft_month_day_90 = Column(INTEGER(11))
    overdraft_balance = Column(DECIMAL(16, 4))
    now_date = Column(Date)


class PcreditCreditGuarantee(Base):
    __tablename__ = 'pcredit_credit_guarantee'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    no = Column(INTEGER(11))
    org = Column(String(20))
    credit_limit = Column(DECIMAL(16, 4))
    card_grant_date = Column(Date)
    amount = Column(DECIMAL(16, 4))
    used_limit = Column(DECIMAL(16, 4))
    bill_date = Column(Date)


class PcreditCreditTaxRecord(Base):
    __tablename__ = 'pcredit_credit_tax_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    org_name = Column(String(64))
    amount = Column(DECIMAL(16, 4))
    stats_date = Column(DateTime)


class PcreditDebit(Base):
    __tablename__ = 'pcredit_debit'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    describe_text = Column(String(200))
    account_status = Column(String(16))
    used_limit = Column(DECIMAL(16, 4))
    avg_limit_6 = Column(DECIMAL(16, 4))
    max_limit = Column(DECIMAL(16, 4))
    paln_repay_amount = Column(DECIMAL(16, 4))
    bill_date = Column(Date)
    actual_repay_amount = Column(DECIMAL(16, 4))
    lately_replay_date = Column(Date)
    overdue_period = Column(INTEGER(11))
    overdue_amount = Column(DECIMAL(16, 4))
    share_amt = Column(DECIMAL(16, 4))
    remarks = Column(String(200))
    repayment_start_year = Column(INTEGER(11))
    repayment_start_month = Column(INTEGER(11))
    repayment_end_year = Column(INTEGER(11))
    repayment_end_month = Column(INTEGER(11))
    overdue_start_year = Column(INTEGER(11))
    overdue_start_month = Column(INTEGER(11))
    overdue_end_year = Column(INTEGER(11))
    overdue_end_month = Column(INTEGER(11))


class PcreditDebitInfo(Base):
    __tablename__ = 'pcredit_debit_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    overdue_amount = Column(DECIMAL(16, 4))
    overdue_times = Column(INTEGER(11))
    overdue_1_less_2 = Column(INTEGER(11))
    overdue_2_less_2 = Column(INTEGER(11))
    overdue_1_greater_2 = Column(INTEGER(11))
    overdue_2_greater_2 = Column(INTEGER(11))
    overdue_3_greater_2 = Column(INTEGER(11))
    overdue_4_greater_2 = Column(INTEGER(11))
    lowest_repayment_count = Column(INTEGER(11))


class PcreditDefaultInfo(Base):
    __tablename__ = 'pcredit_default_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    default_type = Column(String(32))
    default_subtype = Column(String(32))
    default_count = Column(INTEGER(11))
    default_month = Column(INTEGER(11))
    default_balance = Column(INTEGER(11))
    max_overdue_sum = Column(DECIMAL(16, 4))
    max_overdue_month = Column(INTEGER(11))


class PcreditForceExecutionRecord(Base):
    __tablename__ = 'pcredit_force_execution_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    court_name = Column(String(128))
    cause = Column(LONGTEXT)
    register_date = Column(DateTime)
    result_type = Column(String(32))
    case_status = Column(String(32))
    case_end_date = Column(DateTime)
    apply_execution_object = Column(String(128))
    apply_execution_object_amt = Column(DECIMAL(16, 4))
    executed_object = Column(String(128))
    executed_object_amt = Column(DECIMAL(16, 4))


class PcreditFundParticipationRecord(Base):
    __tablename__ = 'pcredit_fund_participation_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    address = Column(String(128))
    pay_date = Column(DateTime)
    total_months = Column(String(32))
    begin_work_date = Column(String(32))
    pay_status = Column(String(32))
    personal_pay_rate = Column(DECIMAL(16, 4))
    monthly_amt = Column(DECIMAL(16, 4))
    pay_company = Column(String(128))
    info_update_date = Column(DateTime)
    reason = Column(String(128))


class PcreditGuaranteeOther(Base):
    __tablename__ = 'pcredit_guarantee_others'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    info = Column(String(200))
    start_date = Column(DateTime)
    bank_name = Column(String(100))
    money_type = Column(INTEGER(11))
    name = Column(String(32))
    contract_amount = Column(DECIMAL(16, 4))
    guarantee_amount = Column(DECIMAL(16, 4))
    loan_balance = Column(DECIMAL(16, 4))
    now_date = Column(Date)


class PcreditGuaranteePay(Base):
    __tablename__ = 'pcredit_guarantee_pay'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    no = Column(INTEGER(11))
    biz_type = Column(String(16))
    debt_date = Column(DateTime)
    debt_amount = Column(DECIMAL(16, 4))
    debt_status = Column(String(32))
    expiry_date = Column(DateTime)
    account_status = Column(String(32))
    account_closedate = Column(DateTime)
    repay_org = Column(String(20))
    lately_repay_date = Column(Date)
    repay_amount = Column(DECIMAL(16, 4))
    lately_repayment_date = Column(Date)
    amount = Column(DECIMAL(16, 4))


class PcreditGuaranteePayDetail(Base):
    __tablename__ = 'pcredit_guarantee_pay_details'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    guapay_id = Column(BIGINT(20))
    special_type = Column(String(32))
    occur_date = Column(DateTime)
    change_month = Column(INTEGER(11))
    occur_amont = Column(DECIMAL(16, 4))
    record = Column(String(128))


class PcreditHouseFundRecord(Base):
    __tablename__ = 'pcredit_house_fund_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    address = Column(String(128))
    pay_date = Column(DateTime)
    pay_month = Column(String(32))
    pay_duration_date = Column(String(32))
    pay_status = Column(String(32))
    month_fee_amt = Column(DECIMAL(16, 4))
    personal_pay_rate = Column(DECIMAL(16, 4))
    company_pay_rate = Column(DECIMAL(16, 4))
    pay_company_name = Column(String(128))
    info_update_date = Column(DateTime)


class PcreditHouseLoan(Base):
    __tablename__ = 'pcredit_house_loan'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    jhi_type = Column(INTEGER(11))
    info = Column(String(200))
    start_date = Column(Date)
    bank_name = Column(String(32))
    money_type = Column(INTEGER(11))
    loan_type = Column(INTEGER(11))
    end_date = Column(Date)
    balance = Column(DECIMAL(16, 4))
    remark = Column(String(200))
    overdue_month_year_5 = Column(INTEGER(11))
    overdue_month_day_90 = Column(INTEGER(11))
    loan_amount = Column(DECIMAL(16, 4))
    overdue_amout = Column(DECIMAL(16, 4))
    now_date = Column(Date)


class PcreditInfo(Base):
    __tablename__ = 'pcredit_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    housing_loan_count = Column(INTEGER(11))
    biz_housing_loan_count = Column(INTEGER(11))
    other_loan_count = Column(INTEGER(11))
    loan_1st_date = Column(String(7))
    debit_card_count = Column(INTEGER(11))
    debit_card_1st_date = Column(String(7))
    semi_credit_card_count = Column(INTEGER(11))
    semi_credit_card_1st_date = Column(INTEGER(11))
    declare_count = Column(INTEGER(11))
    dissent_count = Column(INTEGER(11))
    score = Column(INTEGER(11))
    score_date = Column(String(7))
    bad_debts_count = Column(INTEGER(11))
    bad_debts_balance = Column(DECIMAL(16, 4))
    asset_deal_count = Column(INTEGER(11))
    asset_deal_balance = Column(DECIMAL(16, 4))
    replace_repay_count = Column(INTEGER(11))
    replace_repay_balance = Column(DECIMAL(16, 4))
    loan_overdue_count = Column(INTEGER(11))
    loan_overdue_month = Column(INTEGER(11))
    loan_overdue_month_max_total = Column(DECIMAL(16, 4))
    loan_overdue_max_month = Column(INTEGER(11))
    debit_card_overdue_count = Column(INTEGER(11))
    debit_card_month_count = Column(INTEGER(11))
    debit_card_month_max_total = Column(DECIMAL(16, 4))
    debit_card_max_month = Column(INTEGER(11))
    semi_credit_card_overdraft_acount_60 = Column(INTEGER(11))
    semi_credit_card_overdraft_month_60 = Column(INTEGER(11))
    semi_credit_card_overdraft_balance_60 = Column(DECIMAL(16, 4))
    semi_credit_card_overdraft_max_month_60 = Column(INTEGER(11))
    uncleared_legal_count = Column(INTEGER(11))
    uncleared_org_count = Column(INTEGER(11))
    uncleared_count = Column(INTEGER(11))
    uncleared_contract_count = Column(DECIMAL(16, 4))
    uncleared_balance = Column(DECIMAL(16, 4))
    uncleared_avg_repaly_6 = Column(DECIMAL(16, 4))
    undestroy_legal_count = Column(INTEGER(11))
    undestroy_org_count = Column(INTEGER(11))
    undestroy_count = Column(INTEGER(11))
    undestroy_limit = Column(DECIMAL(16, 4))
    undestory_max_limit = Column(DECIMAL(16, 4))
    undestory_min_limt = Column(DECIMAL(16, 4))
    undestory_used_limit = Column(DECIMAL(16, 4))
    undestory_avg_use = Column(DECIMAL(16, 4))
    undestory_semi_legal_count = Column(INTEGER(11))
    undestory_semi_org_count = Column(INTEGER(11))
    undestory_semi_count = Column(INTEGER(11))
    undestory_semi_limit = Column(DECIMAL(16, 4))
    undestory_semi_max_limit = Column(DECIMAL(16, 4))
    undestory_semi_min_limt = Column(DECIMAL(16, 4))
    undestory_semi_overdraft = Column(DECIMAL(16, 4))
    undestory_semi_avg_overdraft = Column(DECIMAL(16, 4))
    guarantee_count = Column(INTEGER(11))
    guarantee_amont = Column(DECIMAL(16, 4))
    guarantee_principal = Column(DECIMAL(16, 4))
    guarantee_catagory = Column(String(2))
    non_revolloan_org_count = Column(INTEGER(11))
    non_revolloan_accountno = Column(INTEGER(11))
    non_revolloan_totalcredit = Column(DECIMAL(16, 4))
    non_revolloan_balance = Column(DECIMAL(16, 4))
    non_revolloan_repayin_6_m = Column(DECIMAL(16, 4))
    revolcredit_org_count = Column(INTEGER(11))
    revolcredit_account = Column(INTEGER(11))
    revolcredit_totalcredit = Column(DECIMAL(16, 4))
    revolcredit_balance = Column(DECIMAL(16, 4))
    revolcredit_repayin_6_m = Column(DECIMAL(16, 4))
    revolloan_org_count = Column(INTEGER(11))
    revolloan_account_no = Column(INTEGER(11))
    revolloan_totalcredit = Column(DECIMAL(16, 4))
    revolloan_balance = Column(DECIMAL(16, 4))
    revolloan_repayin_6_m = Column(DECIMAL(16, 4))
    ind_guarantee_count = Column(INTEGER(11))
    ind_guarantee_sum = Column(DECIMAL(16, 4))
    ind_guarantee_balance = Column(DECIMAL(16, 4))
    ind_repay_count = Column(INTEGER(11))
    ind_repay_sum = Column(DECIMAL(16, 4))
    ind_repay_balance = Column(DECIMAL(16, 4))
    ent_guarantee_count = Column(INTEGER(11))
    ent_guarantee_sum = Column(DECIMAL(16, 4))
    ent_guarantee_balance = Column(DECIMAL(16, 4))
    ent_repay_count = Column(INTEGER(11))
    ent_repay_sum = Column(DECIMAL(16, 4))
    ent_repay_balance = Column(DECIMAL(16, 4))


class PcreditInsuranceExtractRecord(Base):
    __tablename__ = 'pcredit_insurance_extract_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    address = Column(String(128))
    retirement_category = Column(String(64))
    retirement_date = Column(DateTime)
    begin_work_date = Column(DateTime)
    actual_amt = Column(DECIMAL(16, 4))
    stop_reason = Column(String(128))
    origin_company = Column(String(128))
    info_update_date = Column(DateTime)


class PcreditLargeScale(Base):
    __tablename__ = 'pcredit_large_scale'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    large_scale_quota = Column(DECIMAL(16, 4))
    effective_date = Column(DateTime)
    end_date = Column(DateTime)
    usedsum = Column(DECIMAL(16, 4))


class PcreditLive(Base):
    __tablename__ = 'pcredit_live'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    no = Column(INTEGER(11))
    live_address = Column(String(50))
    phone = Column(String(50))
    live_address_type = Column(String(2))
    update_time = Column(DateTime)


class PcreditLoan(Base):
    __tablename__ = 'pcredit_loan'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    account_type = Column(String(8))
    account_org = Column(String(32))
    account_mark = Column(String(32))
    loan_date = Column(Date)
    end_date = Column(DateTime)
    credit_purpose = Column(String(8))
    respon_object = Column(String(16))
    respon_type = Column(String(16))
    guarantee_no = Column(String(64))
    credit_limit = Column(DECIMAL(16, 4))
    credit_limit_no = Column(String(16))
    credit_share_amt = Column(DECIMAL(16, 4))
    currency = Column(String(16))
    repay_period = Column(INTEGER(11))
    repay_frequency = Column(String(8))
    describe_text = Column(String(200))
    account_status = Column(String(16))
    category = Column(String(8))
    principal_amount = Column(DECIMAL(16, 4))
    surplus_repay_period = Column(INTEGER(11))
    repay_amount = Column(DECIMAL(16, 4))
    plan_repay_date = Column(Date)
    amout_replay_amount = Column(DECIMAL(16, 4))
    lately_replay_date = Column(Date)
    overdue_period = Column(INTEGER(11))
    overdue_amount = Column(DECIMAL(16, 4))
    overdue_31_principal = Column(DECIMAL(16, 4))
    overdue_61_principal = Column(DECIMAL(16, 4))
    overdue_91_principal = Column(DECIMAL(16, 4))
    overdue_180_principal = Column(DECIMAL(16, 4))
    remarks = Column(String(200))
    repayment_start_year = Column(INTEGER(11))
    repayment_start_month = Column(INTEGER(11))
    repayment_end_year = Column(INTEGER(11))
    repayment_end_month = Column(INTEGER(11))
    overdue_start_year = Column(INTEGER(11))
    overdue_start_month = Column(INTEGER(11))
    overdue_end_year = Column(INTEGER(11))
    overdue_end_month = Column(INTEGER(11))
    loan_creditor = Column(String(50))
    loan_amount = Column(DECIMAL(16, 4))
    loan_type = Column(String(30))
    loan_guarantee_type = Column(String(8))
    loan_repay_type = Column(String(20))
    loan_repay_status = Column(String(16))
    joint_loan_mark = Column(String(32))
    expiry_date = Column(DateTime)
    loan_status_time = Column(DateTime)
    quota_used = Column(DECIMAL(16, 4))
    large_scale_balance = Column(DECIMAL(16, 4))
    avg_overdraft_balance_6 = Column(DECIMAL(16, 4))
    max_limit = Column(DECIMAL(16, 4))
    bill_date = Column(DateTime)
    latest_category = Column(String(8))
    latest_loan_balance = Column(DECIMAL(16, 4))
    latest_replay_date = Column(DateTime)
    latest_replay_amount = Column(DECIMAL(16, 4))
    latest_repay_status = Column(String(16))
    loan_expire_date = Column(Date)
    loan_end_date = Column(Date)
    loan_status = Column(String(8))
    loan_balance = Column(DECIMAL(16, 4))


class PcreditLoanGuarantee(Base):
    __tablename__ = 'pcredit_loan_guarantee'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    no = Column(INTEGER(11))
    org = Column(String(20))
    contract_amount = Column(DECIMAL(16, 4))
    loan_grant_date = Column(Date)
    loan_expire_date = Column(Date)
    amount = Column(DECIMAL(16, 4))
    principal_amount = Column(DECIMAL(16, 4))
    category = Column(String(8))
    plan_repay_date = Column(Date)


class PcreditLoanInstitution(Base):
    __tablename__ = 'pcredit_loan_institution'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    org_desc = Column(String(128))
    org_desc_add_date = Column(DateTime)
    jhi_declare = Column(String(128))
    declare_add_date = Column(DateTime)
    remark = Column(String(128))
    remark_add_date = Column(DateTime)
    special_remark = Column(String(128))
    special_remark_date = Column(DateTime)


class PcreditNoncreditDetail(Base):
    __tablename__ = 'pcredit_noncredit_details'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    record_id = Column(BIGINT(20))
    pay_years = Column(INTEGER(11))
    pay_month = Column(INTEGER(11))
    pay_status = Column(String(16))


class PcreditNoncreditInfo(Base):
    __tablename__ = 'pcredit_noncredit_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    noncredit_type = Column(String(10))
    noncredit_count = Column(INTEGER(11))
    noncredit_sum = Column(DECIMAL(16, 4))


class PcreditNoncreditList(Base):
    __tablename__ = 'pcredit_noncredit_list'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    biz_org = Column(String(32))
    biz_type = Column(String(16))
    biz_start_date = Column(DateTime)
    current_payment_status = Column(String(16))
    current_arrears_amt = Column(DECIMAL(16, 4))
    record_date = Column(DateTime)


class PcreditObjectionMark(Base):
    __tablename__ = 'pcredit_objection_mark'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    jhi_type = Column(String(32))
    seq = Column(String(32))
    content = Column(String(128))
    add_date = Column(DateTime)


class PcreditOtherLoan(Base):
    __tablename__ = 'pcredit_other_loan'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    jhi_type = Column(INTEGER(11))
    info = Column(String(200))
    start_date = Column(Date)
    bank_name = Column(String(32))
    money_type = Column(INTEGER(11))
    loan_type = Column(INTEGER(11))
    end_date = Column(Date)
    balance = Column(DECIMAL(16, 4))
    over_line = Column(DECIMAL(16, 4))
    overdue_month_year_5 = Column(INTEGER(11))
    overdue_month_day_90 = Column(INTEGER(11))
    loan_amount = Column(DECIMAL(16, 4))
    now_date = Column(Date)


class PcreditOverdraft(Base):
    __tablename__ = 'pcredit_overdraft'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    jhi_year = Column(INTEGER(11))
    month = Column(INTEGER(11))
    month_amount = Column(INTEGER(11))
    overdue_amount = Column(DECIMAL(16, 4))


class PcreditOverdue(Base):
    __tablename__ = 'pcredit_overdue'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    jhi_year = Column(INTEGER(11))
    month = Column(INTEGER(11))
    month_amount = Column(INTEGER(11))
    overdue_amount = Column(DECIMAL(16, 4))


class PcreditPersonInfo(Base):
    __tablename__ = 'pcredit_person_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    verifi_result = Column(String(64))
    authority = Column(String(64))
    sex = Column(String(8))
    birthday = Column(Date)
    marriage_status = Column(String(8))
    mobile_no = Column(String(20))
    work_tel = Column(String(20))
    home_tel = Column(String(20))
    education = Column(String(8))
    jhi_degree = Column(String(8))
    employment = Column(String(16))
    nationality = Column(String(64))
    email = Column(String(32))
    communication_address = Column(String(50))
    residence_address = Column(String(50))
    spouse_name = Column(String(20))
    spouse_certificate_type = Column(String(16))
    spouse_certificate_no = Column(String(32))
    spouse_work_unit = Column(String(50))
    spouse_mobile_no = Column(String(20))


class PcreditPersonalStatement(Base):
    __tablename__ = 'pcredit_personal_statement'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    content = Column(String(128))
    add_date = Column(DateTime)


class PcreditPhoneHi(Base):
    __tablename__ = 'pcredit_phone_his'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    no = Column(INTEGER(11))
    phone = Column(String(16))
    update_time = Column(DateTime)


class PcreditPortraitsMain(Base):
    __tablename__ = 'pcredit_portraits_main'

    id = Column(BIGINT(20), primary_key=True)
    reprot_id = Column(String(32), nullable=False)
    report_time = Column(DateTime)
    marital_status = Column(INTEGER(11))
    is_owened = Column(INTEGER(11))
    is_mortgage = Column(INTEGER(11))
    is_have_credit = Column(INTEGER(11))


class PcreditPortraitsQuery(Base):
    __tablename__ = 'pcredit_portraits_query'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    loan_approval_inquiry_month_1 = Column(INTEGER(11))
    credit_approval_inquiry_month_1 = Column(INTEGER(11))
    self_inquiry_month_1 = Column(INTEGER(11))
    qualification_examination_year_2 = Column(INTEGER(11))
    loan_approval_month_2 = Column(INTEGER(11))
    credit_approval_month_2 = Column(INTEGER(11))
    loan_approval_month_3 = Column(INTEGER(11))
    credit_approval_month_3 = Column(INTEGER(11))
    loan_approval_month_6 = Column(INTEGER(11))
    credit_approval_month_6 = Column(INTEGER(11))
    loan_approval_year_1 = Column(INTEGER(11))
    credit_approval_year_1 = Column(INTEGER(11))
    qualification_examination_year_1 = Column(INTEGER(11))
    approvals_month_3 = Column(INTEGER(11))


class PcreditPortraitsSummary(Base):
    __tablename__ = 'pcredit_portraits_summary'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    house_loan_count = Column(INTEGER(11))
    first_loan_month = Column(String(10))
    first_credit_month = Column(String(10))
    sum_count = Column(INTEGER(11))
    loan_overdue_count = Column(INTEGER(11))
    loan_max_overdue_money = Column(DECIMAL(16, 4))
    loan_max_overdue_month = Column(INTEGER(11))
    credit_overdue_account_count = Column(INTEGER(11))
    credit_overdue_month_count = Column(INTEGER(11))
    credit_max_overdue_money = Column(DECIMAL(16, 4))
    credit_max_overdue_month = Column(INTEGER(11))
    unsettled_loan_organization_number = Column(INTEGER(11))
    unsettled_loan_number = Column(INTEGER(11))
    unsettled_loan_contract_total = Column(DECIMAL(16, 4))
    unsettled_loan_total_balance = Column(DECIMAL(16, 4))
    unsettled_loan_ave_month_6 = Column(DECIMAL(16, 4))
    uncancelled_credit_organization_number = Column(INTEGER(11))
    uncancelled_credit_total_money = Column(DECIMAL(16, 4))
    uncancelled_credit_max_money = Column(DECIMAL(16, 4))
    uncancelled_credit_used_money = Column(DECIMAL(16, 4))
    uncancellation_credit_average_month_6 = Column(DECIMAL(16, 4))
    uncancelled_quasicredit_account_number = Column(INTEGER(11))
    uncancelled_quasicredit_total_money = Column(DECIMAL(16, 4))
    uncancelled_quasicredit_average_month_6 = Column(DECIMAL(16, 4))
    foreign_guaranty_number = Column(INTEGER(11))
    foreign_guaranty_principal_balance = Column(DECIMAL(16, 4))
    credit_used_rate = Column(DECIMAL(16, 4))


class PcreditPortraitsTransaction(Base):
    __tablename__ = 'pcredit_portraits_transaction'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    loan_account_abnormality = Column(INTEGER(11))
    loan_fivelevel_abnormality = Column(INTEGER(11))
    loan_overdue_month_6 = Column(INTEGER(11))
    loan_overdue_year_2 = Column(INTEGER(11))
    loan_overdue_year_5 = Column(INTEGER(11))
    loan_max_overdue_month = Column(INTEGER(11))
    loan_max_overdue_number = Column(INTEGER(11))
    unsettled_loan_max_overdue_year_2 = Column(INTEGER(11))
    large_loan_overdue_twice = Column(INTEGER(11))
    small_loan_overdue_twice = Column(INTEGER(11))
    business_loan_overdue_twice = Column(INTEGER(11))
    unsettled_loan_unbank_number = Column(INTEGER(11))
    unsettled_busloan_agency_number = Column(INTEGER(11))
    loan_new_total_month_6 = Column(DECIMAL(16, 4))
    loan_new_total_year_1 = Column(DECIMAL(16, 4))
    loan_new_agency_month_3 = Column(INTEGER(11))
    loan_new_agency_year_1 = Column(INTEGER(11))
    guaranteed_loan_total_month_6 = Column(DECIMAL(16, 4))
    guaranteed_loan_total_year_1 = Column(DECIMAL(16, 4))
    guaranteed_loan_agency_year_1 = Column(INTEGER(11))
    loan_expiration_total_month_3 = Column(DECIMAL(16, 4))
    loan_expiration_total_month_6 = Column(DECIMAL(16, 4))
    loan_expiration_total_year_1 = Column(DECIMAL(16, 4))
    business_loan_agency_year_2 = Column(INTEGER(11))
    unsettled_house_loan_number = Column(INTEGER(11))
    unsettled_car_loan_number = Column(INTEGER(11))
    unsettled_house_loan_payed = Column(DECIMAL(16, 4))
    unsettled_car_loan_payed = Column(DECIMAL(16, 4))
    unsettled_loan_overdue_money = Column(DECIMAL(16, 4))
    unsettled_loan_bank_number = Column(INTEGER(11))
    loan_gdz_year_2 = Column(INTEGER(11))
    extension_number = Column(INTEGER(11))
    loan_new_number_month_6 = Column(INTEGER(11))
    loan_new_number_year_1 = Column(INTEGER(11))
    loan_max_overdue_month_6 = Column(INTEGER(11))
    loan_max_overdue_year_1 = Column(INTEGER(11))
    loan_max_overdue_year_2 = Column(INTEGER(11))
    business_loan_corpus_overdue = Column(INTEGER(11))
    loan_doubtful = Column(INTEGER(11))
    credit_account_abnormality = Column(INTEGER(11))
    credit_overdue_month_6 = Column(INTEGER(11))
    credit_overdue_year_2 = Column(INTEGER(11))
    credit_overdue_year_5 = Column(INTEGER(11))
    credit_new_total_year_1 = Column(DECIMAL(16, 4))
    credit_new_total_month_6 = Column(DECIMAL(16, 4))
    credit_gdz_year_2 = Column(INTEGER(11))
    credit_max_overdue_number = Column(INTEGER(11))
    credit_max_overdue_year_2 = Column(INTEGER(11))
    credit_activated_number = Column(INTEGER(11))
    credit_new_number_month_6 = Column(INTEGER(11))
    credit_new_number_year_1 = Column(INTEGER(11))
    credit_now_overdue_money = Column(DECIMAL(16, 4))
    credit_min_payed_number = Column(INTEGER(11))
    credit_quasi_abnormality = Column(INTEGER(11))
    loan_scured_five_abnormality = Column(INTEGER(11))
    credit_financial_tension = Column(DECIMAL(16, 4))


class PcreditProfession(Base):
    __tablename__ = 'pcredit_profession'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    no = Column(INTEGER(11))
    work_unit = Column(String(50))
    work_type = Column(String(64))
    work_phone = Column(String(16))
    work_address = Column(String(50))
    profession = Column(String(20))
    industry = Column(String(20))
    duty = Column(String(10))
    duty_title = Column(String(10))
    enter_date = Column(Date)
    update_time = Column(Date)


class PcreditPubInfo(Base):
    __tablename__ = 'pcredit_pub_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    pub_type = Column(String(32))
    pub_count = Column(INTEGER(11))
    pub_sum = Column(DECIMAL(16, 4))


class PcreditPublicContent(Base):
    __tablename__ = 'pcredit_public_content'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    jhi_comment = Column(String(200))
    detail_id = Column(String(32))


class PcreditPunishmentRecord(Base):
    __tablename__ = 'pcredit_punishment_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    org_name = Column(String(128))
    content = Column(LONGTEXT)
    amount = Column(DECIMAL(16, 4))
    effective_date = Column(DateTime)
    end_date = Column(DateTime)
    reconsideration_result = Column(String(128))


class PcreditQualificationRecord(Base):
    __tablename__ = 'pcredit_qualification_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    qualification_name = Column(String(128))
    grade = Column(String(32))
    award_date = Column(DateTime)
    expired_date = Column(DateTime)
    revoked_date = Column(DateTime)
    award_org = Column(String(128))
    org_address = Column(String(128))


class PcreditQueryRecord(Base):
    __tablename__ = 'pcredit_query_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    no = Column(INTEGER(11))
    jhi_time = Column(Date)
    operator = Column(String(50))
    reason = Column(String(20))


class PcreditQueryTime(Base):
    __tablename__ = 'pcredit_query_times'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    last_query_time = Column(DateTime)
    last_query_org = Column(String(32))
    last_query_type = Column(String(32))
    loan_org_1 = Column(INTEGER(11))
    credit_org_1 = Column(INTEGER(11))
    loan_times_1 = Column(INTEGER(11))
    credit_times_1 = Column(INTEGER(11))
    self_times_1 = Column(INTEGER(11))
    loan_times_2 = Column(INTEGER(11))
    guarantee_times_2 = Column(INTEGER(11))
    agreement_times_2 = Column(INTEGER(11))


class PcreditRepayment(Base):
    __tablename__ = 'pcredit_repayment'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    jhi_year = Column(INTEGER(11))
    month = Column(INTEGER(11))
    repayment_amt = Column(DECIMAL(16, 4))
    status = Column(String(8))


class PcreditRewardRecord(Base):
    __tablename__ = 'pcredit_reward_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    reward_org = Column(String(128))
    reward_content = Column(String(128))
    effective_date = Column(DateTime)
    expired_date = Column(DateTime)


class PcreditScoreInfo(Base):
    __tablename__ = 'pcredit_score_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    score = Column(INTEGER(11))
    position = Column(String(32))
    desc_content = Column(String(64))


class PcreditSemiCredit(Base):
    __tablename__ = 'pcredit_semi_credit'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    describe_text = Column(String(200))
    account_status = Column(String(16))
    overdraft_balance = Column(DECIMAL(16, 4))
    avg_overdraft_balance_6 = Column(DECIMAL(16, 4))
    max_overdraft_amount = Column(DECIMAL(16, 4))
    bill_date = Column(Date)
    actual_repay_amount = Column(DECIMAL(16, 4))
    lately_repay_date = Column(Date)
    overdraft_amount_180 = Column(DECIMAL(16, 4))
    share_amt = Column(DECIMAL(16, 4))
    remarks = Column(String(200))
    repayment_start_year = Column(INTEGER(11))
    repayment_start_month = Column(INTEGER(11))
    repayment_end_year = Column(INTEGER(11))
    repayment_end_month = Column(INTEGER(11))
    overdraft_start_year = Column(INTEGER(11))
    overdraft_start_month = Column(INTEGER(11))
    overdraft_end_year = Column(INTEGER(11))
    overdraft_end_month = Column(INTEGER(11))


class PcreditSimplePortrait(Base):
    __tablename__ = 'pcredit_simple_portraits'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    house_count = Column(INTEGER(11))
    un_house_count = Column(INTEGER(11))
    house_amout = Column(DECIMAL(16, 4))
    un_house_amount = Column(DECIMAL(16, 4))
    un_house_balance = Column(DECIMAL(16, 4))
    un_other_count = Column(INTEGER(11))
    un_other_amount = Column(DECIMAL(16, 4))
    un_other_balance = Column(DECIMAL(16, 4))
    other_max_amout = Column(DECIMAL(16, 4))
    loan_car_count = Column(INTEGER(11))
    un_credit_amount = Column(DECIMAL(16, 4))
    un_credit_bank_amount = Column(DECIMAL(16, 4))
    un_credit_used = Column(DECIMAL(16, 4))
    foreign_guaranty_count = Column(INTEGER(11))
    foreign_guaranty_amount = Column(DECIMAL(16, 4))
    overdue_house_count = Column(INTEGER(11))
    overdue_other_count = Column(INTEGER(11))
    overdue_credit_count = Column(INTEGER(11))
    loan_current_amount = Column(DECIMAL(16, 4))
    credit_current_amount = Column(DECIMAL(16, 4))
    loan_overdue_day_90 = Column(INTEGER(11))
    credit_overdue_day_90 = Column(INTEGER(11))
    un_loan_year_5 = Column(INTEGER(11))
    un_credit_overdue_year_5 = Column(INTEGER(11))
    overdue_house_year_5 = Column(INTEGER(11))
    overdue_other_year_5 = Column(INTEGER(11))
    overdue_credit_year_5 = Column(INTEGER(11))
    credit_amount_month_6 = Column(DECIMAL(16, 4))
    credit_count_month_6 = Column(INTEGER(11))
    credit_amount_month_3 = Column(DECIMAL(16, 4))
    loan_amount_month_6 = Column(DECIMAL(16, 4))
    loan_count_month_6 = Column(INTEGER(11))
    loan_amount_month_3 = Column(DECIMAL(16, 4))
    loan_amount_future_6 = Column(DECIMAL(16, 4))
    loan_count_query_6 = Column(INTEGER(11))
    loan_count_query_3 = Column(INTEGER(11))
    credit_count_query_6 = Column(INTEGER(11))
    credit_count_query_3 = Column(INTEGER(11))
    self_count_query_6 = Column(INTEGER(11))
    self_count_query_3 = Column(INTEGER(11))
    report_time = Column(DateTime)
    self_house_count = Column(INTEGER(11))
    self_fund_count = Column(INTEGER(11))
    un_house_month_pay = Column(DECIMAL(16, 4))
    clear_house_loan_amout = Column(DECIMAL(16, 4))
    un_house_loan_rate = Column(DECIMAL(16, 4))
    un_car_month_pay = Column(DECIMAL(16, 4))
    clear_car_amount = Column(DECIMAL(16, 4))
    clear_other_count = Column(INTEGER(11))
    un_other_loan_count = Column(INTEGER(11))
    un_other_limit_min = Column(DECIMAL(16, 4))
    un_other_limit_max = Column(DECIMAL(16, 4))
    clear_other_limit_min = Column(DECIMAL(16, 4))
    clear_other_limit_max = Column(DECIMAL(16, 4))
    other_month_amount = Column(DECIMAL(16, 4))
    credit_use_rate = Column(DECIMAL(16, 4))
    pb_loan_max_amount = Column(DECIMAL(16, 4))
    credit_report_month = Column(INTEGER(11))
    credit_yuan_count = Column(INTEGER(11))
    overdue_count_year_5 = Column(INTEGER(11))
    un_pb_overdue_count = Column(INTEGER(11))
    loan_amount_future_2 = Column(DECIMAL(16, 4))
    approve_count_month_3 = Column(INTEGER(11))
    approve_count_month_6 = Column(INTEGER(11))
    approve_count_year_2 = Column(INTEGER(11))
    credit_is_normal = Column(INTEGER(11))
    loan_is_normal = Column(INTEGER(11))
    inter_buzy_bank_limit_6_m = Column(DECIMAL(16, 4))
    inter_buzy_bank_limit_max = Column(DECIMAL(16, 4))
    inter_buzy_bank_count_6_m = Column(INTEGER(11))
    other_mont_fixed_payment = Column(DECIMAL(16, 4))
    other_mont_insert_first = Column(DECIMAL(16, 4))
    clear_other_amount_year_1 = Column(DECIMAL(16, 4))
    clear_other_amount_year_2 = Column(DECIMAL(16, 4))
    clear_other_amount_year_3 = Column(DECIMAL(16, 4))
    clear_other_amount_year_4 = Column(DECIMAL(16, 4))
    clear_other_amount_year_1_max = Column(DECIMAL(16, 4))
    clear_other_amount_year_2_max = Column(DECIMAL(16, 4))
    clear_other_amount_year_3_max = Column(DECIMAL(16, 4))
    clear_other_amount_year_4_max = Column(DECIMAL(16, 4))
    credit_report_loan_month = Column(INTEGER(11))
    pb_loan_amount_future_12 = Column(DECIMAL(16, 4))
    loan_amount_future_12 = Column(DECIMAL(16, 4))
    overdue_un_mont_fixed = Column(INTEGER(11))
    un_loan_continued = Column(INTEGER(11))


class PcreditSpecial(Base):
    __tablename__ = 'pcredit_special'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    special_type = Column(String(32))
    special_date = Column(DateTime)
    special_month = Column(INTEGER(11))
    special_money = Column(DECIMAL(16, 4))
    special_comment = Column(String(80))


class PcreditSpeculateRecord(Base):
    __tablename__ = 'pcredit_speculate_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    record_id = Column(BIGINT(20))
    pre_start_date = Column(DateTime)
    pre_end_date = Column(DateTime)
    pre_acc_org = Column(INTEGER(11))
    pre_amt = Column(DECIMAL(16, 4))
    pre_balance = Column(DECIMAL(16, 4))
    pre_freq = Column(INTEGER(11))
    pre_terms = Column(INTEGER(11))
    pre_should_amt = Column(DECIMAL(16, 4))
    pre_real_amt = Column(DECIMAL(16, 4))
    pre_res_terms = Column(INTEGER(11))
    pre_should_date = Column(DateTime)
    pre_month_days = Column(INTEGER(11))
    pre_quar_days = Column(INTEGER(11))
    pre_real_date = Column(DateTime)
    pre_month_period = Column(INTEGER(11))
    pre_quar_period = Column(INTEGER(11))
    loan_repay_type = Column(String(32))
    nominal_interest_rate = Column(DECIMAL(16, 4))
    real_interest_rate = Column(DECIMAL(16, 4))
    settled = Column(BIT(1))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class PcreditTelecomPaymentRecord(Base):
    __tablename__ = 'pcredit_telecom_payment_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    carrier = Column(String(32))
    biz_type = Column(String(128))
    biz_start_date = Column(DateTime)
    current_payment_status = Column(String(32))
    current_arrears_amt = Column(DECIMAL(16, 4))
    current_arrears_months = Column(String(32))
    record_date = Column(DateTime)
    last_24_month_payment_history = Column(String(512))


class PcreditThresholdRecord(Base):
    __tablename__ = 'pcredit_threshold_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    personal_category = Column(String(32))
    address = Column(String(128))
    work_place = Column(String(128))
    home_monthly_income = Column(DECIMAL(16, 4))
    apply_date = Column(DateTime)
    approval_date = Column(DateTime)
    info_update_date = Column(DateTime)


class PcreditTransaction(Base):
    __tablename__ = 'pcredit_transaction'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    remarks = Column(String(200))


class PcreditWarnInfo(Base):
    __tablename__ = 'pcredit_warn_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    warn_content = Column(String(256))
    effective_date = Column(DateTime)
    expiry_date = Column(DateTime)


class RawDatum(Base):
    __tablename__ = 'raw_data'

    id = Column(BIGINT(20), primary_key=True)
    channel_no = Column(String(20))
    api_no = Column(String(20))
    raw_data_desc = Column(String(64))
    risk_subject_id = Column(BIGINT(20))
    expired_at = Column(DateTime)
    req_message = Column(Text)
    req_msg_check_sum = Column(String(512))
    data_type = Column(String(50))
    raw_data = Column(MEDIUMTEXT)
    cause = Column(String(64))
    create_time = Column(DateTime)


class RequestRecord(Base):
    __tablename__ = 'request_record'

    id = Column(BIGINT(20), primary_key=True)
    biz_type = Column(String(50))
    api_no = Column(String(20))
    request_id = Column(String(50))
    user_name = Column(String(50))
    risk_subject_id = Column(BIGINT(20))
    user_seq = Column(String(64))
    merchant_no = Column(String(64))
    request_param = Column(Text)
    request_status = Column(String(50))
    data_fetch_mode = Column(String(50))
    create_time = Column(DateTime)


class RiskSubject(Base):
    __tablename__ = 'risk_subject'

    id = Column(BIGINT(20), primary_key=True)
    user_type = Column(String(255))
    name = Column(String(64))
    phone = Column(String(15))
    unique_no = Column(String(64))
    reg_code = Column(String(64))
    create_time = Column(DateTime)
    modify_time = Column(DateTime)


class TransAccount(Base):
    __tablename__ = 'trans_account'

    id = Column(BIGINT(20), primary_key=True)
    out_req_no = Column(String(32))
    account_name = Column(String(32))
    id_card_no = Column(String(32))
    id_type = Column(String(32))
    bank = Column(String(32))
    account_no = Column(String(64))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    trans_flow_type = Column(INTEGER(11))
    update_time = Column(DateTime)
    account_state = Column(INTEGER(11))
    create_time = Column(DateTime)


class TransApply(Base):
    __tablename__ = 'trans_apply'

    id = Column(BIGINT(20), primary_key=True)
    out_req_no = Column(String(32))
    report_req_no = Column(String(32))
    apply_no = Column(String(32))
    cus_name = Column(String(32))
    related_name = Column(String(32))
    relationship = Column(String(32))
    account_id = Column(BIGINT(20))
    industry = Column(String(32))
    id_card_no = Column(String(32))
    id_type = Column(String(32))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransFlow(Base):
    __tablename__ = 'trans_flow'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    out_req_no = Column(String(32))
    trans_time = Column(DateTime)
    opponent_name = Column(String(255))
    trans_amt = Column(DECIMAL(16, 4))
    account_balance = Column(DECIMAL(16, 4))
    currency = Column(String(16))
    opponent_account_no = Column(String(32))
    opponent_account_bank = Column(String(16))
    trans_channel = Column(String(16))
    trans_type = Column(String(16))
    trans_use = Column(String(16))
    remark = Column(String(32))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransFlowPortrait(Base):
    __tablename__ = 'trans_flow_portrait'

    id = Column(BIGINT(20), primary_key=True)
    flow_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    account_id = Column(BIGINT(20))
    trans_date = Column(DateTime)
    trans_time = Column(DateTime)
    trans_amt = Column(DECIMAL(16, 4))
    account_balance = Column(DECIMAL(16, 4))
    opponent_name = Column(String(32))
    opponent_type = Column(INTEGER(11))
    opponent_account_no = Column(String(32))
    opponent_account_bank = Column(String(32))
    trans_channel = Column(String(32))
    trans_type = Column(String(32))
    trans_use = Column(String(32))
    remark = Column(String(32))
    currency = Column(String(16))
    phone = Column(String(32))
    relationship = Column(String(32))
    is_financing = Column(INTEGER(11))
    is_interest = Column(INTEGER(11))
    loan_type = Column(String(32))
    is_repay = Column(INTEGER(11))
    is_before_interest_repay = Column(INTEGER(11))
    unusual_trans_type = Column(String(16))
    is_sensitive = Column(INTEGER(11))
    cost_type = Column(String(16))
    remark_type = Column(String(32))
    income_cnt_order = Column(INTEGER(11))
    expense_cnt_order = Column(INTEGER(11))
    income_amt_order = Column(INTEGER(11))
    expense_amt_order = Column(INTEGER(11))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransSingleAbnormalRecovery(Base):
    __tablename__ = 'trans_single_abnormal_recovery'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    flow_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    opponent_name = Column(String(32))
    account_no = Column(String(64))
    abnormal_recovery_id = Column(BIGINT(20))
    abnormal_recovery_label = Column(String(32))
    trans_amt = Column(DECIMAL(16, 4))
    trans_datetime = Column(DateTime)
    remark = Column(String(32))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransSingleCounterpartyPortrait(Base):
    __tablename__ = 'trans_single_counterparty_portrait'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    month = Column(String(16))
    opponent_name = Column(String(32))
    income_amt_order = Column(String(16))
    expense_amt_order = Column(String(16))
    trans_amt = Column(DECIMAL(16, 4))
    trans_month_cnt = Column(INTEGER(11))
    trans_cnt = Column(INTEGER(11))
    trans_mean = Column(DECIMAL(16, 4))
    trans_amt_proportion = Column(DECIMAL(16, 4))
    trans_gap_avg = Column(DECIMAL(16, 4))
    income_amt_proportion = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransSingleLoanPortrait(Base):
    __tablename__ = 'trans_single_loan_portrait'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    loan_type = Column(String(16))
    month = Column(String(16))
    loan_amt = Column(DECIMAL(16, 4))
    loan_cnt = Column(INTEGER(11))
    loan_mean = Column(DECIMAL(16, 4))
    repay_amt = Column(DECIMAL(16, 4))
    repay_cnt = Column(INTEGER(11))
    repay_mean = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransSinglePortrait(Base):
    __tablename__ = 'trans_single_portrait'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    analyse_start_time = Column(DateTime)
    analyse_end_time = Column(DateTime)
    not_full_month = Column(String(16))
    normal_income_amt = Column(DECIMAL(16, 4))
    normal_income_cnt = Column(INTEGER(11))
    normal_income_mean = Column(INTEGER(11))
    normal_income_d_mean = Column(INTEGER(11))
    normal_income_m_mean = Column(INTEGER(11))
    normal_income_m_std = Column(INTEGER(11))
    normal_expense_amt = Column(DECIMAL(16, 4))
    normal_expense_cnt = Column(INTEGER(11))
    income_amt_y_pred = Column(DECIMAL(16, 4))
    relationship_risk = Column(INTEGER(11))
    income_0_to_5_cnt = Column(INTEGER(11))
    income_5_to_10_cnt = Column(INTEGER(11))
    income_10_to_30_cnt = Column(INTEGER(11))
    income_30_to_50_cnt = Column(INTEGER(11))
    income_50_to_100_cnt = Column(INTEGER(11))
    ncome_100_to_200_cnt = Column(INTEGER(11))
    income_above_200_cnt = Column(INTEGER(11))
    balance_0_to_5_day = Column(INTEGER(11))
    balance_5_to_10_day = Column(INTEGER(11))
    balance_10_to_30_day = Column(INTEGER(11))
    balance_30_to_50_day = Column(INTEGER(11))
    balance_50_to_100_day = Column(INTEGER(11))
    balance_100_to_200_day = Column(INTEGER(11))
    balance_above_200_day = Column(INTEGER(11))
    income_weight_max = Column(DECIMAL(16, 4))
    income_weight_min = Column(DECIMAL(16, 4))
    balance_weight_max = Column(DECIMAL(16, 4))
    balance_weight_min = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransSingleRelatedPortrait(Base):
    __tablename__ = 'trans_single_related_portrait'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    opponent_name = Column(String(32))
    relationship = Column(String(32))
    income_cnt_order = Column(INTEGER(11))
    income_cnt = Column(INTEGER(11))
    income_amt_order = Column(INTEGER(11))
    income_amt = Column(DECIMAL(16, 4))
    income_amt_proportion = Column(DECIMAL(16, 4))
    expense_cnt_order = Column(INTEGER(11))
    expense_cnt = Column(INTEGER(11))
    expense_amt_order = Column(INTEGER(11))
    expense_amt = Column(DECIMAL(16, 4))
    expense_amt_proportion = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransSingleRemarkPortrait(Base):
    __tablename__ = 'trans_single_remark_portrait'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    remark_type = Column(String(32))
    remark_income_amt_order = Column(INTEGER(11))
    remark_expense_amt_order = Column(INTEGER(11))
    remark_trans_cnt = Column(INTEGER(11))
    remark_trans_amt = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransSingleSummaryPortrait(Base):
    __tablename__ = 'trans_single_summary_portrait'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    month = Column(String(16))
    q_1_year = Column(INTEGER(11))
    q_2_year = Column(INTEGER(11))
    q_3_year = Column(INTEGER(11))
    q_4_year = Column(INTEGER(11))
    normal_income_amt = Column(DECIMAL(16, 4))
    normal_expense_amt = Column(DECIMAL(16, 4))
    net_income_amt = Column(DECIMAL(16, 4))
    salary_cost_amt = Column(DECIMAL(16, 4))
    living_cost_amt = Column(DECIMAL(16, 4))
    tax_cost_amt = Column(DECIMAL(16, 4))
    rent_cost_amt = Column(DECIMAL(16, 4))
    insurance_cost_amt = Column(DECIMAL(16, 4))
    loan_cost_amt = Column(DECIMAL(16, 4))
    interest_amt = Column(DECIMAL(16, 4))
    balance_amt = Column(DECIMAL(16, 4))
    interest_balance_proportion = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransUAbnormalRecovery(Base):
    __tablename__ = 'trans_u_abnormal_recovery'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(255))
    account_id = Column(BIGINT(20))
    flow_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    opponent_name = Column(String(64))
    account_no = Column(String(64))
    abnormal_recovery_id = Column(BIGINT(20))
    abnormal_recovery_label = Column(String(32))
    trans_amt = Column(DECIMAL(16, 4))
    trans_datetime = Column(DateTime)
    remark = Column(String(64))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransUCounterpartyPortrait(Base):
    __tablename__ = 'trans_u_counterparty_portrait'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(32))
    report_req_no = Column(String(32))
    month = Column(String(16))
    opponent_name = Column(String(64))
    income_amt_order = Column(String(16))
    expense_amt_order = Column(String(16))
    trans_amt = Column(DECIMAL(16, 4))
    trans_month_cnt = Column(INTEGER(11))
    trans_cnt = Column(INTEGER(11))
    trans_mean = Column(DECIMAL(16, 4))
    trans_amt_proportion = Column(DECIMAL(16, 4))
    trans_gap_avg = Column(DECIMAL(16, 4))
    income_amt_proportion = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransUFlowPortrait(Base):
    __tablename__ = 'trans_u_flow_portrait'

    id = Column(BIGINT(20), primary_key=True)
    flow_id = Column(BIGINT(20))
    apply_no = Column(String(32))
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    trans_date = Column(DateTime)
    trans_time = Column(DateTime)
    trans_amt = Column(DECIMAL(16, 4))
    account_balance = Column(DECIMAL(16, 4))
    bank = Column(String(64))
    account_no = Column(String(64))
    opponent_name = Column(String(64))
    opponent_type = Column(INTEGER(11))
    opponent_account_no = Column(String(64))
    opponent_account_bank = Column(String(64))
    trans_channel = Column(String(64))
    trans_type = Column(String(32))
    trans_use = Column(String(64))
    remark = Column(String(64))
    currency = Column(String(16))
    phone = Column(String(16))
    relationship = Column(String(32))
    is_financing = Column(INTEGER(11))
    is_interest = Column(INTEGER(11))
    is_repay = Column(INTEGER(11))
    is_before_interest_repay = Column(INTEGER(11))
    loan_type = Column(String(16))
    unusual_trans_type = Column(String(16))
    is_sensitive = Column(INTEGER(11))
    cost_type = Column(String(16))
    remark_type = Column(String(64))
    income_cnt_order = Column(INTEGER(11))
    expense_cnt_order = Column(INTEGER(11))
    income_amt_order = Column(INTEGER(11))
    expense_amt_order = Column(INTEGER(11))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransULoanPortrait(Base):
    __tablename__ = 'trans_u_loan_portrait'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(32))
    report_req_no = Column(String(32))
    loan_type = Column(String(32))
    month = Column(String(16))
    loan_amt = Column(DECIMAL(16, 4))
    loan_cnt = Column(INTEGER(11))
    loan_mean = Column(DECIMAL(16, 4))
    repay_amt = Column(DECIMAL(16, 4))
    repay_cnt = Column(INTEGER(11))
    repay_mean = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransUModelling(Base):
    __tablename__ = 'trans_u_modelling'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(32))
    report_req_no = Column(String(32))
    apply_amt = Column(DECIMAL(16, 4))
    pawn_cnt = Column(INTEGER(11))
    medical_cnt = Column(INTEGER(11))
    court_cnt = Column(INTEGER(11))
    insure_cnt = Column(INTEGER(11))
    night_trans_cnt = Column(INTEGER(11))
    fam_unstab_cnt = Column(INTEGER(11))
    balance_mean = Column(DECIMAL(16, 4))
    balance_max = Column(DECIMAL(16, 4))
    balance_max_0_to_5 = Column(DECIMAL(16, 4))
    balance_0_to_5_prop = Column(DECIMAL(16, 4))
    income_0_to_5_prop = Column(DECIMAL(16, 4))
    balance_min_weight = Column(DECIMAL(16, 4))
    balance_max_weight = Column(DECIMAL(16, 4))
    income_max_weight = Column(DECIMAL(16, 4))
    half_year_interest_amt = Column(DECIMAL(16, 4))
    half_year_balance_amt = Column(DECIMAL(16, 4))
    year_interest_amt = Column(DECIMAL(16, 4))
    q_2_balance_amt = Column(DECIMAL(16, 4))
    q_3_balance_amt = Column(DECIMAL(16, 4))
    year_interest_balance_prop = Column(DECIMAL(16, 4))
    q_4_interest_balance_prop = Column(DECIMAL(16, 4))
    income_mean = Column(DECIMAL(16, 4))
    mean_sigma_left = Column(DECIMAL(16, 4))
    mean_sigma_right = Column(DECIMAL(16, 4))
    mean_2_sigma_left = Column(DECIMAL(16, 4))
    mean_2_sigma_right = Column(DECIMAL(16, 4))
    normal_income_mean = Column(DECIMAL(16, 4))
    normal_income_amt_d_mean = Column(DECIMAL(16, 4))
    normal_income_amt_m_mean = Column(DECIMAL(16, 4))
    normal_expense_amt_m_std = Column(DECIMAL(16, 4))
    opponent_cnt = Column(INTEGER(11))
    income_rank_1_amt = Column(DECIMAL(16, 4))
    income_rank_2_amt = Column(DECIMAL(16, 4))
    income_rank_3_amt = Column(DECIMAL(16, 4))
    income_rank_4_amt = Column(DECIMAL(16, 4))
    income_rank_2_cnt_prop = Column(DECIMAL(16, 4))
    expense_rank_6_avg_gap = Column(DECIMAL(16, 4))
    income_rank_9_avg_gap = Column(DECIMAL(16, 4))
    expense_rank_10_avg_gap = Column(DECIMAL(16, 4))
    relationship_risk = Column(INTEGER(11))
    enterprise_3_income_amt = Column(DECIMAL(16, 4))
    enterprise_3_expense_cnt_prop = Column(DECIMAL(16, 4))
    all_relations_expense_cnt_prop = Column(DECIMAL(16, 4))
    hit_loan_type_cnt_6_cm = Column(INTEGER(11))
    private_income_amt_12_cm = Column(DECIMAL(16, 4))
    private_income_mean_12_cm = Column(DECIMAL(16, 4))
    pettyloan_income_amt_12_cm = Column(DECIMAL(16, 4))
    pettyloan_income_mean_12_cm = Column(DECIMAL(16, 4))
    finlease_expense_cnt_6_cm = Column(INTEGER(11))
    otherfin_income_mean_3_cm = Column(DECIMAL(16, 4))
    all_loan_expense_cnt_3_cm = Column(DECIMAL(16, 4))
    income_net_rate_compare_2 = Column(DECIMAL(16, 4))
    cus_apply_amt_pred = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransUPortrait(Base):
    __tablename__ = 'trans_u_portrait'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(255))
    report_req_no = Column(String(32))
    analyse_start_time = Column(DateTime)
    analyse_end_time = Column(DateTime)
    not_full_month = Column(String(16))
    normal_income_amt = Column(DECIMAL(16, 4))
    normal_income_cnt = Column(INTEGER(11))
    normal_income_mean = Column(DECIMAL(16, 4))
    normal_income_d_mean = Column(DECIMAL(16, 4))
    normal_income_m_mean = Column(DECIMAL(16, 4))
    normal_income_m_std = Column(DECIMAL(16, 4))
    normal_expense_amt = Column(DECIMAL(16, 4))
    normal_expense_cnt = Column(INTEGER(11))
    income_amt_y_pred = Column(DECIMAL(16, 4))
    relationship_risk = Column(INTEGER(11))
    income_0_to_5_cnt = Column(INTEGER(11))
    income_5_to_10_cnt = Column(INTEGER(11))
    income_10_to_30_cnt = Column(INTEGER(11))
    income_30_to_50_cnt = Column(INTEGER(11))
    income_50_to_100_cnt = Column(INTEGER(11))
    income_100_to_200_cnt = Column(INTEGER(11))
    income_above_200_cnt = Column(INTEGER(11))
    balance_0_to_5_day = Column(INTEGER(11))
    balance_5_to_10_day = Column(INTEGER(11))
    balance_10_to_30_day = Column(INTEGER(11))
    balance_30_to_50_day = Column(INTEGER(11))
    balance_50_to_100_day = Column(INTEGER(11))
    balance_100_to_200_day = Column(INTEGER(11))
    balance_above_200_day = Column(INTEGER(11))
    income_weight_max = Column(DECIMAL(16, 4))
    income_weight_min = Column(DECIMAL(16, 4))
    balance_weight_max = Column(DECIMAL(16, 4))
    balance_weight_min = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransURelatedPortrait(Base):
    __tablename__ = 'trans_u_related_portrait'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(32))
    report_req_no = Column(String(32))
    opponent_name = Column(String(64))
    relationship = Column(String(16))
    income_cnt_order = Column(INTEGER(11))
    income_cnt = Column(INTEGER(11))
    income_amt_order = Column(INTEGER(11))
    income_amt = Column(DECIMAL(16, 4))
    income_amt_proportion = Column(DECIMAL(16, 4))
    expense_cnt_order = Column(INTEGER(11))
    expense_cnt = Column(INTEGER(11))
    expense_amt_order = Column(INTEGER(11))
    expense_amt = Column(DECIMAL(16, 4))
    expense_amt_proportion = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransURemarkPortrait(Base):
    __tablename__ = 'trans_u_remark_portrait'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(32))
    report_req_no = Column(String(32))
    remark_type = Column(String(64))
    remark_income_amt_order = Column(INTEGER(11))
    remark_expense_amt_order = Column(INTEGER(11))
    remark_trans_cnt = Column(INTEGER(11))
    remark_trans_amt = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransUSummaryPortrait(Base):
    __tablename__ = 'trans_u_summary_portrait'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(255))
    report_req_no = Column(String(32))
    month = Column(String(16))
    q_1_year = Column(INTEGER(11))
    q_2_year = Column(INTEGER(11))
    q_3_year = Column(INTEGER(11))
    q_4_year = Column(INTEGER(11))
    normal_income_amt = Column(DECIMAL(16, 4))
    normal_expense_amt = Column(DECIMAL(16, 4))
    net_income_amt = Column(DECIMAL(16, 4))
    salary_cost_amt = Column(DECIMAL(16, 4))
    living_cost_amt = Column(DECIMAL(16, 4))
    tax_cost_amt = Column(DECIMAL(16, 4))
    rent_cost_amt = Column(DECIMAL(16, 4))
    insurance_cost_amt = Column(DECIMAL(16, 4))
    loan_cost_amt = Column(DECIMAL(16, 4))
    interest_amt = Column(DECIMAL(16, 4))
    balance_amt = Column(DECIMAL(16, 4))
    interest_balance_proportion = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransactionBill(Base):
    __tablename__ = 'transaction_bill'

    id = Column(BIGINT(20), primary_key=True)
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)
    user_name = Column(String(64))
    phone = Column(String(15))
    id_card_no = Column(String(64))
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    create_time = Column(DateTime)
    modify_time = Column(DateTime)
    bank_card_no = Column(String(25))
    jhi_type = Column(String(50))
    file_path = Column(String(255))
    card_type = Column(String(50))


class TransactionBillLine(Base):
    __tablename__ = 'transaction_bill_line'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    create_time = Column(DateTime)
    modify_time = Column(DateTime)
    transaction_bill_id = Column(BIGINT(20))
    serial_no = Column(String(64))
    band_card_no = Column(String(32))
    id_card_no = Column(String(64))
    user_name = Column(String(64))
    phone = Column(String(15))
    gen_card_bank = Column(String(64))
    master_type = Column(String(50))
    currency = Column(String(50))
    transaction_time = Column(DateTime)
    payee_user_name = Column(String(32))
    payee_bank_info = Column(String(64))
    payee_bank_card = Column(String(32))
    purpose = Column(String(32))
    transaction_mode = Column(String(50))
    summary = Column(String(32))
    channel = Column(String(50))
    available_amount = Column(DECIMAL(16, 4))
    on_line_audit = Column(INTEGER(11))
    verification = Column(BIT(1))
    batch_no = Column(String(64))
    import_mode = Column(INTEGER(11))


class JhiPersistentAuditEvtDatum(Base):
    __tablename__ = 'jhi_persistent_audit_evt_data'

    event_id = Column(ForeignKey('jhi_persistent_audit_event.event_id'), primary_key=True, nullable=False, index=True)
    name = Column(String(150), primary_key=True, nullable=False)
    value = Column(String(255))

    event = relationship('JhiPersistentAuditEvent')
