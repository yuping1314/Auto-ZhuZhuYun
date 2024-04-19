# encoding: utf-8
'''
@file: cleaning.py
@author: yuping
@time: 2023/1/7/007 13:28
'''
from utils.read_config import ReadConfig
from utils.dbmanager import DBManager
import logging
from utils.logger import logger



class Cln(object):
    def __init__(self,df):
        self.df=df
        self.config = ReadConfig().conf_ini()
        self.logger = logger()
        self.db = DBManager(self.logger, self.config, choose_db='db')
    def _clean_same_data(self):
        self.df.loc[self.df['lc_company'].str.contains('EMS'), 'lc_company'] = 'ems'
        self.df.loc[self.df['lc_company'].str.contains('邮政'), 'lc_company'] = '邮政'
        self.df.loc[self.df['lc_company'].str.contains('百世'), 'lc_company'] = '百世'
        self.df.loc[self.df['lc_company'].str.contains('韵达'), 'lc_company'] = '韵达'
        self.df.loc[self.df['lc_company'].str.contains('圆通'), 'lc_company'] = '圆通'
        self.df.loc[self.df['lc_company'].str.contains('申通'), 'lc_company'] = '申通'
        self.df.loc[self.df['lc_company'].str.contains('中通'), 'lc_company'] = '中通'
        self.df.loc[self.df['lc_company'].str.contains('极兔'), 'lc_company'] = '极兔'
        self.df.loc[self.df['lc_company'].str.contains('京东'), 'lc_company'] = '京东' 
        self.df.loc[self.df['lc_company'].str.contains('壹米滴答'), 'lc_company'] = '壹米滴答'
        self.df.loc[self.df['lc_company'].str.contains('中铁快运'), 'lc_company'] = '中铁快运'
        self.df.loc[self.df['lc_company'].str.contains('安能物流'), 'lc_company'] = '安能物流'

        self.df['l_id'].replace(to_replace='@', value='', inplace=True, regex=True)  
        self.df['status']=0
        self.df = self.df.apply(
            lambda x: x.fillna(0) if x.dtype.kind in 'fi' else x.fillna('') if x.dtype.kind in 'O' else x)

    def clean_diff_data(self):
       pass
    def _save_db(self):

        self.df = self.df.apply(
            lambda x: x.fillna(0) if x.dtype.kind in 'fi' else x.fillna('') if x.dtype.kind in 'O' else x)
        update_key = ['u_time','status', 'remark']
        update_key.extend(self.df.columns)
        table_name = 'ods_zzy_temp_upload'
        self.db.executemany(df=self.df, table_name=table_name, execute_type='on duplicate key update',
                                   update_key=update_key)


class ClnFdr(Cln):
    def __init__(self,df):
        
        super(ClnFdr, self).__init__(df)
    def clean_data(self):

        self._clean_same_data()

        self._save_db()

        
class ClnDB(Cln):
    def __init__(self,df):
        
        super(ClnDB, self).__init__(df)
    def clean_data(self):
        self._clean_same_data()
        self._save_db()


class ClnFtry(object):
    def __init__(self):
        pass
    def cleaning(self):
        pass


class ClnFdrFtry(ClnFtry):
    '''工厂模式 清洗文件夹数据'''
    def __init__(self):
        super(ClnFdrFtry, self).__init__()
    def cleaning(self):
        return  ClnFdr
class ClnDBFtry(ClnFtry):
    '''工厂模式 清洗数据库数据'''
    def __init__(self):
        super(ClnDBFtry, self).__init__()
    def cleaning(self):
        return  ClnDB


