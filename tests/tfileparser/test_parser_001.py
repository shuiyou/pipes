from app import sql_db
from fileparser.Parser001 import Parser001


file = r"../resource/trans_flow/账号6226190200138646交易明细070902.xls"
param = {
    'bankAccount': '6226190200138646',
    'cusName': '赵高枫',
    'idNo': '666666888888'
}

file_list = [r"../resource/trans_flow/王伟明.xlsx", r"../resource/trans_flow/农行流水.xlsx",
             r"../resource/trans_flow/建行流水.xlsx", r"../resource/trans_flow/王伟芳.xlsx"]
param_list = [
    {
        # 'appId': '商户端ID',
        'cusType': "PERSONAL",
        'cusName': "王伟明",
        'idNo': "202007091402563453",
        'idType': "ID_CARD_NO",
        'bankAccount': "6228480038574396172",
        'bankName': "工商银行",
        # 'outApplyNo': "申请业务号",
        # 'outReqNo': "外部请求编号",
        # 'bizReqNo': "回执编号",
        # 'accountId': "账户编号"
    },
    {
        # 'appId': '商户端ID',
        'cusType': "PERSONAL",
        'cusName': "王伟明",
        'idNo': "202007091402563453",
        'idType': "ID_CARD_NO",
        'bankAccount': "66666688888888",
        'bankName': "农业银行",
        # 'outApplyNo': "申请业务号",
        # 'outReqNo': "外部请求编号",
        # 'bizReqNo': "回执编号",
        # 'accountId': "账户编号"
    },
    {
        # 'appId': '商户端ID',
        'cusType': "PERSONAL",
        'cusName': "王伟明",
        'idNo': "202007091402563453",
        'idType': "ID_CARD_NO",
        'bankAccount': "88888888666666",
        'bankName': "建设银行",
        # 'outApplyNo': "申请业务号",
        # 'outReqNo': "外部请求编号",
        # 'bizReqNo': "回执编号",
        # 'accountId': "账户编号"
    },
    {
        # 'appId': '商户端ID',
        'cusType': "PERSONAL",
        'cusName': "王伟芳",
        'idNo': "202007001406374532",
        'idType': "ID_CARD_NO",
        'bankAccount': "6222620110014694302",
        'bankName': "交通银行",
        # 'outApplyNo': "申请业务号",
        # 'outReqNo': "外部请求编号",
        # 'bizReqNo': "回执编号",
        # 'accountId': "账户编号"
    }
]


def test_001():
    p = Parser001()
    p.file = file
    p.param = param
    p.sql_db = sql_db()
    resp = p.process()
    print(resp)


def test_002():
    p = Parser001()
    p.file = file_list[2]
    p.param = param_list[2]
    p.sql_db = sql_db()
    resp = p.process()
    print(resp)


def test_003():
    p = Parser001()
    p.sql_db = sql_db()
    for i in range(4):
        p.file = file_list[i]
        p.param = param_list[i]
        resp = p.process()
        print(resp)
