# encoding: utf-8
'''
@file: logger.py
@author: yuping
@time: 2023/9/11/008 14:22
'''

import os
import logging
from utility_class.system.loggingutil import LoggerClass

def logger():
    
    abspath=os.path.abspath('.')
    log_path=abspath+r'\log'
    loggerclass = LoggerClass(level=logging.INFO, output_level='both', path=log_path)
    logger = loggerclass.logger()
    return logger
