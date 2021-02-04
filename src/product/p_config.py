
# 湛泸产品编码和决策process的对应关系
product_code_process_dict = {
    "001": "Level1_m",  # 一级个人报告
    "002": "Level1_m",  # 一级企业
    "003": "Level1_m",   # 一级联合报告
    "004": "Level1_m",   # 风险拦截
    "005": "Grey_list",   # 灰名单移出
    "06001": "POST_LOAN",  # 贷后报告
    "07001": "CREDIT_REP",    # 征信报告
    "07002": "CREDIT_REP",     # 征信拦截
    "08001": "TRANS_FLOW",     # 流水报告
    "08002": "TRANS_FLOW",      # 流水拦截
    "03002": "LEVEL_ONE",  # 新版一级联合报告
    "07003": "CREDIT_COM",    # 企业征信报告
    "07004": "CREDIT_COM",    # 企业征信拦截
    "04002": "LEVEL_ONE",  # 新版一级联合报告
    "09001": "S_QUICK_LOAN",  # 小额快贷风险拦截
}

# 在这个dict里针对不同的产品设置他的详情字段转换
product_code_view_dict = {
    "001": ['10001', '13001', '12001', '17001', '07001', '09001', 'f0003'],
    "003": ['10001', '13001', '12001', '17001', '07001', '09001', 'f0003'],
    "004": ['10001', '13001', '12001', '17001', '07001', '09001', 'f0003', "14001"],
    "06001": ['10001', '13001', '12001', '17001', '07001', '09001', 'f0003', "14001"],
    "07001": ['41001'],
    "08001": ['51001'],
    "08002": ['51001']
}

# 产品对应需要 mapping的变量清洗。
product_codes_dict = {
    "07001": ['41001'],
    "07002": ['41001'],
    "08001": ['51001'],
    "08002": ['51001'],
    "07003": ['41003'],
    "07004": ['41003'],
}
