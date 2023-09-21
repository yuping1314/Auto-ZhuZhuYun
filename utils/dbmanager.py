# encoding: utf-8
'''
@file: dbmanager.py
@author: yuping
@time: 2023/1/8/008 15:22
'''
'''
管理数据库的pandas读取和executemany写入
'''
import configparser,time
import pandas as pd
import pymysql

class DBManager(object):
    def __init__(self, logger, config, choose_db='db'):
        '''
        初始化数据库
        建立连接、游标
        '''
        self.logger = logger
        self.config = config
        self.choose_db = choose_db 

    def _connect_db(self):
        self.user = self.config.get(self.choose_db, 'user')
        self.password = self.config.get(self.choose_db, 'password')
        self.host = self.config.get(self.choose_db, 'host')
        self.port = self.config.getint(self.choose_db, 'port')
        self.database = self.config.get(self.choose_db, 'database')
        
        self.conn=pymysql.connect(user=self.user,password=self.password,host=self.host,port=self.port,database=self.database)
        
        self.cur = self.conn.cursor()
        
    def _close_db(self):
        '''
        关闭游标、关闭数据库
        '''
        self.cur.close()
        self.conn.close()
        

    def pandas_read_db(self,sql_cmd):
        t = 1
        while True:
            if t <= 3:
                try:

                    self._connect_db()
                    engine='mysql+pymysql://{user}:{password}@{host}:{port}/{database}'.format(
                    host=self.host,port=self.port,user=self.user,password=self.password,database=self.database)
                    df=pd.read_sql(sql=sql_cmd,con=engine)
                    return df

                except Exception as e:
                    
                    self.logger.error(f'pandas_read_db第{t}次运行失败',exc_info=True)
                    time.sleep(10)
                    t += 1  
            else:
                self.logger.error('pandas_read_db运行三次仍旧出错，请检查！') 
                break

    def pandas_save_db(self,table_name,df):
        '''可以用来快速入库，和快速建表'''
        t = 1
        while True:
            if t <= 3:
                try:

                    self._connect_db()
                    engine = 'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'.format(
                        host=self.host, port=self.port, user=self.user, password=self.password, database=self.database)
                    df.to_sql(name=table_name, con=engine)

                    break
                except Exception as e:
                    self.logger.error(f'pandas_save_db第{t}次运行失败',exc_info=True)
                    time.sleep(10)
                    t += 1  
            else:
                self.logger.error('pandas_save_db运行三次仍旧出错，请检查！')  
                break

    def execute(self,sql_cmd,type=None):
        '''
        执行单步语句
        :return:
        '''
        t = 1
        while True:
            if t <= 3:
                try:

                    self._connect_db()
                    try:
                        self.logger.debug(f'正在执行{sql_cmd}')
                        self.result=self.cur.execute(sql_cmd)
                        if type=='select fetchone': 
                            
                            return self.cur.fetchone() 
                        
                        
                        if type == 'select fetchall':
                            return self.cur.fetchall()
                        self.conn.commit()
                        


                    except :
                        self.logger.error('执行execute语句失败！', exc_info=True)
                        self.conn.rollback()
                    finally:
                        self._close_db()



                    break
                except Exception as e:
                    
                    self.logger.error(f'execute第{t}次运行失败',exc_info=True)
                    time.sleep(10)
                    t += 1  
            else:
                self.logger.error('execute运行三次仍旧出错，请检查！') 
                break

    def executemany(self,df,table_name,execute_type='replace into',update_key=None):
        '''
        同时执行多条语句，批量插入
        update_key 使用sql语句快速生成，然后删掉主键，唯一索引，c_time等不需要更新的列,注意,u_time需要更新
                    SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR "','")
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = 'sh_data' AND TABLE_NAME = 'ods_api_shop';

        更新的列
        update_key = ['u_time', 'remark']
        update_key.extend(df_product.columns)
        '''
        t = 1
        while True:
            if t <= 3:
                try:

                    self._connect_db()
                    
                    
                    value_lists = [[None if pd.isna(y) else y for y in x] for x in df.values] 
                    columns_str = ','.join(df.columns)
                    
                    s_count = '%s,' * len(df.columns)
                    if execute_type=='insert ignore into':
                        sql_cmd = '''insert ignore into {} ({}) values({})'''.format(table_name, columns_str, s_count[:-1])
                    elif execute_type=='replace into':
                        sql_cmd = '''replace into {} ({}) values({})'''.format(table_name, columns_str, s_count[:-1])
                        
                    else: 
                        
                        update_dict = {key: 'values({key})'.format(key=key) for key in update_key}
                        
                        update_list = ["{key}={value}".format(key=key, value=value) for key, value in update_dict.items()]
                        
                        update_str = ','.join(update_list)
                        sql_cmd="insert into {table}({keys}) values ({values}) on duplicate key update {update_str}"\
                                            .format(table=table_name, keys=columns_str, values=s_count[:-1],update_str=update_str)

                    try:
                        self.logger.debug(f'正在执行{sql_cmd}')
                        self.cur.execute(f'ALTER TABLE `{table_name}` AUTO_INCREMENT =1') 
                        self.cur.executemany(sql_cmd, value_lists)
                        self.conn.commit()
                        
                    except :
                        self.logger.error('执行executemany语句失败,正在回滚！', exc_info=True)
                        self.conn.rollback()
                    finally:
                        self._close_db()

                    break
                except Exception as e:
                    
                    self.logger.error(f'executemany第{t}次运行失败',exc_info=True)     
                    time.sleep(10)
                    t += 1  
            else:
                self.logger.error('executemany运行三次仍旧出错，请检查！')  
                break

if __name__ == '__main__':
    from utility_class.system.loggingutil import LoggerClass
    import logging

    loggerclass = LoggerClass(level=logging.DEBUG,output_level='both',path=r'E:\py\logs')
    logger = loggerclass.logger()
    dbmanager=DBManager(logger)

    sql='select * from jst_export_oms_order limit 10'
    a=dbmanager.execute(sql)
    


