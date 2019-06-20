from mapping.mapper import read_product, translate, get_transformer


def test_read_product():
    product = read_product('RP_P_L1')
    codes = product['code'].unique()
    print(codes)
    print(product.head())
    print(product['变量名'])


def test_type():
    t05002 = get_transformer('05002')
    t05002.transform(user_name='张虎', id_card_no='430122197512087812')
    print(t05002.variables)
    assert t05002.variables['ps_name_id'] == 0


def test_transform():
    res = translate(product_code='RP_P_L1', user_name='张虎', id_card_no='430122197512087812')
    print(res)
