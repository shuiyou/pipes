import json

import pandas as pd

from util.defensor_client import DefensorClient


def test_json_2_df():
    info = '''[{"id": 1, "appId": "0000000000", "riskDetail": "\u6cd5\u9662\u5931\u4fe1\u660e\u5355", "strategyCode": "222,333,4444", "dataService": "\u5931\u4fe1\u540d\u5355", "createTime": "2019-10-16T02:50:06Z", "modifyTime": "2019-10-16T02:50:11Z"}]'''
    df = pd.read_json(info)
    print(df)


def test_json_2_df1():
    df_client = DefensorClient(None)
    df_client.app_id = "0000000000"
    df_client.grey_list_query_url = "http://localhost:9781/api/open/grey-list/hit"

    data = df_client.query_grey_list("小明", "61242938382828347", "ID_CARD_NO")
    info = json.dumps(data)
    df = pd.read_json(info)
    print(df)
