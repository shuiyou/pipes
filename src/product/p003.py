from exceptions import ServerException
from logger.logger_util import LoggerUtil
from product.generate import Generate
from flask import request, jsonify
import json
from mapping.t00000 import T00000
from product.p_utils import _build_request

logger = LoggerUtil().logger(__name__)



class P003(Generate):

    def __init__(self)->None:
        super().__init__()
        self.reponse: {}

    def shack_hander_process(self):
        try:
            json_data = request.get_json()
            logger.debug(json.dumps(json_data))
            req_no = json_data.get('reqNo')
            product_code = json_data.get('productCode')
            query_data_array = json_data.get('queryData')
            #遍历query_data_array调用strategy
            for data in query_data_array:
                queryParam = data.get_json()
                user_name = queryParam.get('name')
                id_card_no = queryParam.get('idno')
                phone = queryParam.get('phone')
                user_type = queryParam.get('userType')
                variables = T00000().run(user_name, id_card_no, phone, user_type)['variables']
                # 决策要求一直要加上00000，用户基础信息。
                variables['out_strategyBranch'] = '00000'
                strategy_request = _build_request(req_no, product_code, variables=variables)
                logger.info(strategy_request)

        except Exception as err:
            logger.error(str(err))
            raise ServerException(code=500, description=str(err))


    def strategy_process(self):
        try:
            pass
        except Exception as err:
            logger.error(str(err))
            raise ServerException(code=500, description=str(err))

