
# 湛泸产品编码和决策process的对应关系
product_code_process_dict = {
    "001": "Level1_m",  # 一级个人报告
    "002": "Level1_m",  # 一级企业
    "003": "Level1_m",   # 一级联合报告
    "004": "Level1_m",   # 风险拦截
    "005": "Grey_list"   # 灰名单移出
}

# 在这个dict里针对不同的产品设置他的详情字段转换
product_code_view_dict = {
    # 一级个人报告
    "001": ['10001', '13001', '12001', '17001', '07001', '09001', 'f0003'],
    "003": ['10001', '13001', '12001', '17001', '07001', '09001', 'f0003'],
    "004": ['10001', '13001', '12001', '17001', '07001', '09001', 'f0003']
}