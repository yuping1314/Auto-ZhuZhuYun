# encoding: utf-8
'''
@file: get.py
@author: yuping
@time: 2023/1/7/007 13:12
'''

import os
import pandas as pd
from utils.dbmanager import DBManager

from utils.read_config import ReadConfig
import logging
from utils.logger import logger


class Read(object):
    def __init__(self):
        self.logger = logger()
        self.config = ReadConfig().conf_ini()
        self.db = DBManager(logger, self.config, choose_db='db')


class ReadFolder(Read):
    def __init__(self):
        super(ReadFolder, self).__init__()
        self.folder_path = self.config.get('path', 'wait_query')

    def read(self):
        df_list = []
        for root, dirs, files in os.walk(self.folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                df = pd.read_excel(file_path)
                df_list.append(df)
        # print(df_list, 23456)
        dfs = pd.concat(df_list)
        dfs = dfs.dropna(subset=['快递公司', '快递单号'])

        dfs.rename(columns={'快递公司': 'lc_company', '快递单号': 'l_id'}, inplace=True)
        return dfs


class ReadDB(Read):
    def __init__(self):
        super(ReadDB, self).__init__()

    def read(self, sql_cmd):
        df = self.db.pandas_read_db(sql_cmd)
        df.rename(columns={'快递公司': 'lc_company', '快递单号': 'l_id'}, inplace=True)

        return df


class ReadFactory(object):
    def __init__(self):
        pass

    def get_data(self, source_type):
        if source_type == 'folder':
            return ReadFolder
        if source_type == 'DB':
            return ReadDB
