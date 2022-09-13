import logging
import os, time

class Log(object):
    def __init__(self, log_file=None, log_level=None,log_path="/tmp/logs",log_name="tmd.log"):
        self.log_level = log_level if log_level else "DEBUG"
        self.log_file = log_file if log_file else os.path.join(log_path, log_name)
        if not os.path.exists(log_path):os.mkdir(log_path)
        self.logger = logging.getLogger()
        if (self.log_level == "DEBUG"):
            self.logger.setLevel(logging.DEBUG)
        elif (self.log_level == "INFO"):
            self.logger.setLevel(logging.INFO)
        elif (self.log_level == "WARNING"):
            self.logger.setLevel(logging.WARNING)
        elif (self.log_level == "ERROR"):
            self.logger.setLevel(logging.ERROR)
        self.formatter = logging.Formatter('[%(asctime)s] - %(levelname)s: %(message)s',datefmt='%m-%d-%y %H:%M:%S')

    def __log(self, level, message):
        fh = logging.FileHandler(self.log_file, 'a', encoding='utf-8')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(self.formatter)
        self.logger.addHandler(fh)

        if level == 'debug':
            self.logger.debug(message)
        elif level == 'info':
            self.logger.info(message)
        elif level == 'warning':
            self.logger.warning(message)
        elif level == 'error':
            self.logger.error(message)

        self.logger.removeHandler(fh)
        
        fh.close()

    def debug(self, message):
        self.__log('debug', message)

    def info(self, message):
        self.__log('info', message)

    def warning(self, message):
        self.__log('warning', message)

    def error(self, message):
        self.__log('error', message)


if __name__ == '__main__':
    log = Log(log_level="DEBUG")
    log.debug("test info")
    log.info("test start")
    log.warning("test end")