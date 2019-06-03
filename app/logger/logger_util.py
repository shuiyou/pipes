import logging
import os
from logging.config import fileConfig


class LoggerUtil:
    __LOGGER_CONFIG = None

    def logger(self, module_name):
        if self.__LOGGER_CONFIG is None:
            app_env = os.getenv("ENV", 'prod').lower()
            print("app_env is :" + app_env)
            self.__LOGGER_CONFIG = fileConfig('logger/logging-' + app_env + '.conf')
        logger = logging.getLogger(module_name)
        return logger
