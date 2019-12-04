# @Time : 2019/10/16 2:25 PM
# @Author : lixiaobo
# @Site :
# @File : defensor_client.py
# @Software: PyCharm
from abc import ABCMeta

import requests
from py_eureka_client import eureka_client

from exceptions import ServerException
from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)


class DefensorClient(object):
    __metaclass__ = ABCMeta

    def __init__(self, headers) -> None:
        if headers is not None:
            self.app_id = headers.get("appId")
            self.grey_list_query_url = headers.get("greyListQueryUrl")
            logger.info("appId:%s", self.app_id)
            logger.info("grey_list_query_url:%s", self.grey_list_query_url)
        else:
            self.app_id = None
            self.grey_list_query_url = None
        logger.debug("DefensorClient module name:%s", __name__)

    def query_grey_list(self, name, id_no, id_type, hit=None):
        logger.info("query_grey_list begin.")
        if self.grey_list_query_url is None or self.app_id is None:
            logger.debug("query_grey_list nothing to do, param: grey_list_query_url or appId is empty.")
            return
        param = {"appId": self.app_id, "name": name, "idType": id_type, "idno": id_no}
        if hit is not None:
            param[hit] = hit

        logger.info("query_grey_list param:%s", param)
        resp = requests.post(self.grey_list_query_url, data=param)
        logger.info("query_grey_list resp:%s", resp.content.decode())
        if resp.status_code != 200:
            raise ServerException(description="查询灰名单异常", response=str(resp.content.decode()), code="500")

        response_json = resp.json()
        resCode = response_json.get("resCode")
        if resCode != 0:
            error_info = "查询灰名单出错，resCode:" + resCode + "message:" + response_json.get("message")
            raise ServerException(description="查询灰名单状态码不正确", response=error_info, code="500")
        return response_json.get("data")

    def invoke_api(self, param):
        res = eureka_client.walk_nodes("DEFENSOR", "/api/open/grey-list/hit", walker=self.do_invoke_api)

    def do_invoke_api(self, url, param):
        pass
