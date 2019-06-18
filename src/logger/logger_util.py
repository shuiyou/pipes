import logging
import os
from logging.config import fileConfig


class LoggerUtil:
    __LOGGER_CONFIG = None

    def logger(self, module_name):
        if self.__LOGGER_CONFIG is None:
            app_env = os.getenv("ENV", 'prod').lower()
            print("app_env is :" + app_env)
            root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            conf_file = os.path.join(root_dir, 'logger', 'logging-' + app_env + '.conf')
            self.__LOGGER_CONFIG = fileConfig(fname=conf_file)
        logger = logging.getLogger(module_name)
        return logger
