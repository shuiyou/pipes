import json

import pandas as pd

from util.defensor_client import DefensorClient


def test_json_2_df():
    info = '''[{"id": 1, "appId": "0000000000", "riskDetail": "法院失信名单", "strategyCode": "222,333,4444", "dataService": "\u5931\u4fe1\u540d\u5355", "createTime": "2019-10-16T02:50:06Z", "modifyTime": "2019-10-16T02:50:11Z"}]'''
    df = pd.read_json(info)
    print("\n")
    print(df.query('riskDetail == "法院失信名单"').empty)

    risk_detail = "法院失信名单"
    print(not df.query('riskDetail=="' + risk_detail + '"').empty)


def test_json_2_df1():
    df_client = DefensorClient(None)
    df_client.app_id = "9999999999"
    df_client.grey_list_query_url = "http://192.168.1.15:100/gateway/defensor/api/open/grey-list/hit"

    data = df_client.query_grey_list("孙洋洋", "340321198704272796", "ID_CARD_NO")
    info = json.dumps(data)
    df = pd.read_json(info)
    print(df)
