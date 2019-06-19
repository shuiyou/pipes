from mapping.short_message import Shortmessage


def test_hd_reg_cnt():
    c1 = Shortmessage(user_name = '孙文杰', id_card_no = '371302198610273437', phone = '12345678912')
    result = c1.hd_reg_cnt()
    print("result is:", result)

