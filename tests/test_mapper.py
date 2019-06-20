from mapping.mapper import read_product


def test_read_product():
    product = read_product('湛泸一级清洗数据')
    codes = product['code'].unique()
    print(codes)
    print(product.head())
