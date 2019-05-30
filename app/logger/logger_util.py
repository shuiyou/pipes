import logging
from logging.config import fileConfig


class LoggerUtil:
    __LOGGER_CONFIG = None

    def logger(self, module_name):
        if self.__LOGGER_CONFIG is None:
            self.__LOGGER_CONFIG = fileConfig('logging.conf')
        logger = logging.getLogger(module_name)
        return logger
