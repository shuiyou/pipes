from logger.logger_util import LoggerUtil


def test_log_stash():
    logger_util = LoggerUtil()
    logger = logger_util.logger(__name__)
    logger.info("test my logger....")
