import logging

# import pandas as pd
# from metabasepy import Client
import pandas as pd
from metabasepy import Client

from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)
logger.info('python-logstash-async: tests logstash info message.')

token = None
with open("token.txt", "r") as myfile:
    lines = myfile.readlines()
    if len(lines) > 0:
        token = lines[0]

cli = Client(username='luokui@magfin.cn', password="laokai1981", base_url="http://192.168.1.37:3000", token=token)
if token is None:
    cli.authenticate()
    with open("token.txt", "w") as token_file:
        token_file.write(cli.token)


def read_as_df(database_name, query):
    database_name = cli.databases.get_by_name(database_name)
    card_id = cli.cards.post(database_id=database_name[0]['id'], name=database_name[0]['name'], query=query)
    query_response = cli.cards.download(card_id=card_id, format='json')
    cli.cards.delete(card_id=card_id)
    return pd.DataFrame(query_response)


df = read_as_df(database_name="金融产品部", query="select cont_amt from acc_loan")
print(df.shape)


