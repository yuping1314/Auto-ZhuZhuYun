# encoding: utf-8
'''
@file: read_config.py
@author: yuping
@time: 2023/9/11/007 21:40
'''

import configparser
import os
import yaml


class ReadConfig:
    """定义一个读取配置文件的类"""

    def __init__(self):
        self.abspath = os.path.abspath('.')

    def conf_ini(self, file_path=None):
        '''读取config.ini格式的配置文件'''
        if file_path:
            config_path = file_path
        else:
            config_path = self.abspath + r'\config\config.ini'

        config = configparser.ConfigParser()
        config.read(config_path, encoding='utf-8')
        return config

    def conf_yaml(self, file_path=None):
        if file_path:
            config_path = file_path
        else:
            config_path = self.abspath + '\config\config.yaml'

        file = open(config_path, 'r', encoding='UTF-8')
        config = yaml.load(file, yaml.FullLoader)

        return config
