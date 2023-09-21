# encoding: utf-8
'''
基于logging模块，方便调用logger调用
包括
基础输出日志
文件输出日志
控制台输出日志
文件+控制台输出日志

'''
import logging
import os
import datetime
from datetime import datetime
import sys
import colorlog


class LoggerClass(object):
    log_colors_config = {
        'DEBUG': 'blue',
        'INFO': 'white',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red',
    }

    def __init__(self, level, output_level, path):
        self.level = level
        self.output_level = 'both'
        self.logging = logging
        self.path = path

    def basic_logging(self):
        '''logging简单配置'''
        logging.basicConfig(level=self.level, format=
        '[%(asctime)s]-[%(levelname)s]-[%(filename)s]-[%(lineno)s]：%(message)s')
        return logging

    def file_name(self):
        '''以文件名字的格式为  path/program/xxx.log'''

        today = datetime.now()

        abspath = sys.argv[0]
        p_name = os.path.basename(abspath)
        program_name = os.path.splitext(p_name)[0]

        folder_path = self.path + '\\' + program_name
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        file_name = folder_path + '\\' + today.strftime('%Y%m%d') + '.log'

        return file_name

    def output_console(self):
        '''日志直接输出到控制台'''
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(level=self.level)

        formatter = colorlog.ColoredFormatter(
            '%(log_color)s [%(asctime)s]-[%(levelname)s]-[%(filename)s]-[%(lineno)s]：%(message)s',
            log_colors=self.log_colors_config)
        stream_handler.setFormatter(formatter)
        return stream_handler

    def output_file(self):
        '''输出到文件'''
        file_handler = logging.FileHandler(filename=self.file_name(), mode='a', encoding='utf-8')
        file_handler.setLevel(level=self.level)

        formatter = logging.Formatter(
            '[%(asctime)s]-[%(levelname)s]-[%(filename)s]-[%(lineno)s]：%(message)s')

        file_handler.setFormatter(formatter)
        return file_handler

    def logger(self):
        '''定义logger,传入handler'''

        logger = self.logging.getLogger(__name__)
        logger.setLevel(level=self.level)

        if self.output_level == 'file':
            file_handler = self.output_file()
            logger.addHandler(file_handler)
            return logger
        elif self.output_level == 'console':
            console_handler = self.output_console()
            logger.addHandler(console_handler)
            return logger
        elif self.output_level == 'both':
            file_handler = self.output_file()
            console_handler = self.output_console()
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
            return logger
        else:
            logging = self.basic_logging()
            return logging


if __name__ == '__main__':

    abspath = os.path.abspath('.')
    config_path = abspath + r'\util\config\config.ini'
    log_path = abspath + r'\log'

    loggerclass = LoggerClass(level=logging.DEBUG, output_level='both', path=log_path)
    logger = loggerclass.logger()
    logger.info('测试info~~~')

    try:
        a = 0 / 0
    except:
        logger.error('测试error', exc_info=True)

    logger.debug('测试debug~~~')
