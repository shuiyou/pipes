import json

from mapping.p07001_m.data_prepared_processor import DataPreparedProcessor


def read_file(url):
    f = open(url,'r', encoding='UTF-8')
    content = f.read()
    f.close()
    return json.loads(content)

def run(ps, params):
    user_name = params.get("user_name")
    id_card_no = params.get("id_card_no")
    phone = params.get("phone")
    cached_data = params.get("cached_data")
    origin_data = params.get("origin_data")
    handler = DataPreparedProcessor()
    handler.init(ps.variables, user_name, id_card_no, origin_data=origin_data, cached_data=cached_data)
    handler.process()
    ps.run(user_name=user_name, id_card_no=id_card_no, phone=phone, origin_data=origin_data, cached_data=cached_data)