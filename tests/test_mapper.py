from mapping.mapper import translate, get_transformer


def test_type():
    t05002 = get_transformer('05002')
    t05002.run(user_name='张虎', id_card_no='430122197512087812')
    print(t05002.variables)
    assert t05002.variables['ps_name_id'] == 0


def test_transform():
    codes = ['14001']
    res = translate(codes, user_name='张虎', id_card_no='430122197512087812')
    print(res)
