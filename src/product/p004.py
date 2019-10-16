import json
import traceback

from flask import request
import pandas as pd

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from product.p003 import P003

logger = LoggerUtil().logger(__name__)


class P004(P003):
    def __init__(self) -> None:
        self.reponse: {}

    def shake_hand_process(self):
        try:
            resp_data = self.df_client.query_grey_list(name="小明", id_no="61242938382828347", id_type="ID_CARD_NO")
            content_data = json.dumps(resp_data)
            logger.info("resp_data-" + content_data)
            df = pd.read_json(content_data)
            print(df)

            json_data = request.get_json()
            logger.info("风险拦截BizTypes请求开始...")
            logger.debug("request_data" + str(json.dumps(json_data)))
            req_no = json_data.get('reqNo')
            product_code = json_data.get('productCode')
            query_data_array = json_data.get('queryData')
            response_array = []
            # 遍历query_data_array调用strategy
            for data in query_data_array:
                response_array.append(self._shack_hander_response(data, product_code, req_no))
            resp = {
                'productCode': product_code,
                'reqNo': req_no,
                'queryData': response_array
            }
            self.reponse = resp
            logger.info("response data" + str(self.reponse))
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))
