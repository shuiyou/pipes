from exceptions import ServerException
from logger.logger_util import LoggerUtil
from product.generate import Generate

logger = LoggerUtil().logger(__name__)



class P003(Generate):

    def __init__(self)->None:
        super().__init__()
        self.reponse: {}

    def shack_hander_process(self):
        try:
            pass
        except Exception as err:
            logger.error(str(err))
            raise ServerException(code=500, description=str(err))


    def strategy_process(self):
        try:
            pass
        except Exception as err:
            logger.error(str(err))
            raise ServerException(code=500, description=str(err))

