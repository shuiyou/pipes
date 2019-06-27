from mapping.mapper import translate, get_transformer


def test_type():
    t05002 = get_transformer('05002')
    t05002.run(user_name='张虎', id_card_no='430122197512087812')
    print(t05002.variables)
    assert t05002.variables['ps_name_id'] == 0


def test_transform():
    codes=['00000', '01001','02001','05001','05002','06001', '07001','08001',
           '09001', '10001', '11001', '12001', '13001', '14001', '16001', '16002',
           '17001', '18001', 'f0001', 'f0002', 'f0003']
    res = translate(codes, user_name='张虎', id_card_no='430122197512087812')
    print(res)
