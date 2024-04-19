# encoding: utf-8
'''
@file: start_zhuzhuyun.py
@author: yuping
@time: 2023/9/11 
'''
import os
import shutil
import time
from datetime import datetime, timedelta

import pandas as pd
import schedule
from sqlalchemy import create_engine

from prepare_data.cleaning import ClnDBFtry, ClnFdrFtry
from prepare_data.get import ReadFactory
from query_data.zzy import ZZY
from utils.dbmanager import DBManager
from utils.logger import logger
from utils.read_config import ReadConfig
from utils.sql_cmd import *


def create_table():
    db.execute(sql_creat_upload)
    db.execute(sql_creat_lg_order)


def loop(job, *args):
    t = 1
    while True:
        if t <= 3:
            try:
                job(*args)
                break
            except:

                logger.error(f'第{t}次运行失败，正在重新运行！', exc_info=True)
                time.sleep(60)
                t += 1
        else:
            logger.error('运行三次仍旧出错，退出程序，请检查！', exc_info=True)
            break
    return args[0]


def prepare(*args):
    '''准备数据'''
    source_type = args[0]
    rd = ReadFactory().get_data(source_type=args[0])
    if source_type == 'folder':
        df = rd().read()
        cleaning = ClnFdrFtry().cleaning()
        cleaning(df).clean_data()
    else:
        df = rd().read(sql_cmd=args[1])
        cleaning = ClnDBFtry().cleaning()
        cleaning(df).clean_data()


def query(source_type, upload_start_time):
    '''猪猪云查询'''
    zzy = ZZY(upload_start_time)
    zzy.start(source_type)


def export_data(df_main):
    '''
    df_main:列(快递公司，快递单号）
    输出预发货/物流预警等查询的需要快递公司进行假揽收的订单
    '''

    export_folder_path = r'Y:\huojian\揽收\快递公司_未揽收'
    backups_folder_path = r'X:\huojian\data\JST\history\快递公司_未揽收'
    for i in os.listdir(export_folder_path):
        f_path = export_folder_path + '\\' + i
        f_new_path = backups_folder_path + '\\' + i
        shutil.move(f_path, f_new_path)
    date_hour = datetime.strftime(datetime.now(), '%Y_%m_%d %H_00')
    df_main['剔除列'] = df_main.apply(lambda x:
                                   '剔除' if (x['快递公司'].find('安能') >= 0) |
                                           (x['快递公司'].find('壹米滴答') >= 0) |
                                           (x['快递公司'].find('中通快运') >= 0)
                                   else '',
                                   axis=1
                                   )
    df_main = df_main[df_main['剔除列'] != '剔除']

    df_youzheng_ems = df_main.loc[df_main['快递公司'].str.contains('邮政|EMS'), :]
    df_youzheng_ems = df_youzheng_ems.rename(columns={'快递公司': '快递公司'})
    df_youzheng_ems = df_youzheng_ems[['快递公司', '快递单号']]
    if df_youzheng_ems.shape[0] != 0:
        df_youzheng_ems.to_excel(f'{export_folder_path}\邮政和EMS未揽收_{date_hour}.xlsx', index=False)
        df_youzheng_ems.to_excel(f'Y:\huojian\揽收\邮政_未揽收\邮政和EMS未揽收_{date_hour}.xlsx', index=False)
    df_jitu_baishi = df_main.loc[df_main['快递公司'].str.contains('极兔|百世'), :]
    df_jitu_baishi = df_jitu_baishi.rename(columns={'快递公司': '快递公司'})
    df_jitu_baishi = df_jitu_baishi[['快递公司', '快递单号']]
    if df_jitu_baishi.shape[0] != 0:
        df_jitu_baishi.to_excel(f'{export_folder_path}\极兔和百世未揽收_{date_hour}.xlsx', index=False)

    df_zhuji_st_yd = df_main.loc[df_main['快递公司'].str.contains('诸暨申通|诸暨韵达'), :]
    df_zhuji_st_yd = df_zhuji_st_yd.rename(columns={'快递公司': '快递公司'})
    df_zhuji_st_yd = df_zhuji_st_yd[['快递公司', '快递单号']]
    if df_zhuji_st_yd.shape[0] != 0:
        df_zhuji_st_yd.to_excel(f'{export_folder_path}\诸暨申通和诸暨韵达未揽收_{date_hour}.xlsx', index=False)

    df_jiangsushentong = df_main.loc[df_main['快递公司'].str.contains('江苏申通')]
    df_jiangsushentong = df_jiangsushentong.rename(columns={'快递公司': '快递公司'})
    df_jiangsushentong = df_jiangsushentong[['快递公司', '快递单号']]
    if df_jiangsushentong.shape[0] != 0:
        df_jiangsushentong.to_excel(f'{export_folder_path}\江苏东台申通未揽收_{date_hour}.xlsx', index=False)

    l_id_list = []
    l_id_list.extend(list(df_youzheng_ems['快递单号'].unique()))
    l_id_list.extend(list(df_jitu_baishi['快递单号'].unique()))
    l_id_list.extend(list(df_zhuji_st_yd['快递单号'].unique()))
    l_id_list.extend(list(df_jiangsushentong['快递单号'].unique()))
    df_daifa = df_main.loc[~df_main['快递单号'].isin(l_id_list)]
    df_daifa = df_daifa.rename(columns={'快递公司': '快递公司'})
    df_daifa = df_daifa[['快递公司', '快递单号']]
    if df_daifa.shape[0] != 0:
        df_daifa.to_excel(f'{export_folder_path}\代发未揽收_{date_hour}.xlsx', index=False)
    logger.info(f"未揽收订单共计 {df_main.shape[0]} 单")
    logger.info(f"邮政+EMS {df_youzheng_ems.shape[0]} 单")
    logger.info(f"极兔+百世 {df_jitu_baishi.shape[0]} 单")
    logger.info(f"诸暨申通+诸暨韵达 {df_zhuji_st_yd.shape[0]} 单")
    logger.info(f"代发 {df_daifa.shape[0]} 单")
    logger.info(f"江苏东台申通 {df_jiangsushentong.shape[0]} 单")


def job1():
    upload_start_time = (datetime.now() - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
    source_type = loop(prepare, 'folder')

    loop(query, source_type, upload_start_time)
    logger.info(f'job1任务完成！')


def job2(sql_cmd):
    '''根据sql_cmd不同，上传不同数量的数据'''
    upload_start_time = (datetime.now() - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
    source_type = loop(prepare, 'DB', sql_cmd)

    loop(query, source_type, upload_start_time)
    logger.info(f'job2任务完成！')


def job3(sql_cmd1, sql_cmd2):
    '''查询物流预警订单'''
    '''查询预发货订单'''
    upload_start_time = (datetime.now() - timedelta(minutes=5)).strftime(
        '%Y-%m-%d %H:%M:%S')
    source_type = loop(prepare, 'DB', sql_cmd1)

    loop(query, source_type, upload_start_time)

    logger.info("正在按快递公司输出未揽收快递单号~~~")
    df = pd.read_sql(sql_cmd2, sh_data_engine)
    if not df.empty:
        export_data(df)
    else:
        logger.error("未获取到数据，请检查！！！")
    logger.info(f'job3任务完成！')


def job4(sql_cmd1, sql_cmd2):
    '''查询预发货订单'''
    upload_start_time = (datetime.now() - timedelta(minutes=5)).strftime(
        '%Y-%m-%d %H:%M:%S')
    source_type = loop(prepare, 'DB', sql_cmd1)

    loop(query, source_type, upload_start_time)

    logger.info("正在按快递公司输出未揽收快递单号~~~")
    df = pd.read1100084012906_sql(sql_cmd2, sh_data_engine)
    if not df.empty:
        export_data(df)
    else:
        logger.error("未获取到数据，请检查！！！")
    logger.info(f'job4任务完成！')


if __name__ == '__main__':
    logger = logger()
    logger.info('程序开始运行~~~')
    config = ReadConfig().conf_ini()
    db = DBManager(logger, config)
    sh_data_engine = create_engine(
        'mysql+pymysql://fl_senhubi:fl1886870@222.188.126.45:12306/sh_data')

    job1()  # 读取文件夹，进行查询
    # job2(sql_upload_ytd) #昨天订单
    # job2(sql_upload_30day) #近30天订单
    # job3(sql_lg_warning_order1, sql_lg_warning_not_receive1)  # 9点未揽收
    # job4(sql_pr1100084012906e_send_order1,sql_pre_send_not_receive1) #13点未揽收
    # job4(sql_pre_send_order2,sql_pre_send_not_receive2) #18点未揽收

    schedule.every().day.at('23:30:00').do(job1)  # 按需，单独执行文件夹一次，次日手动取消定时

    schedule.every().day.at('09:10:00').do(job3, sql_lg_warning_order1, sql_lg_warning_not_receive1)
    schedule.every().day.at('13:10:00').do(job4, sql_pre_send_order1, sql_pre_send_not_receive1)
    schedule.every().day.at('18:10:00').do(job4, sql_pre_send_order2, sql_pre_send_not_receive2)

    schedule.every().day.at('06:00:00').do(job2, sql_upload_ytd)

    # while True:9889437019531988943905389998899889437216487988943661605898516142279949889439072822988943842730098894374877599889437023844988943721647098894379056049889437060651988943905390098894363313639898894370930579889436616067988943746446998516140293769889436988896988943767513898894374785529889436443475988943909159398894363362599889437067423988943644347398894366062619851623519855988943772258298894370363419889436954335988943815279898894361905769889436916212988943865458398894379106279889439095923988943787357798894372164699889438171755988943629763898894369503569889437736976988943555473197735341755409889435200297985162352013098894376408769889438148241988943748776498894366949819889438556292988943904476698894366258239889439063489988943614368689437193927988943893620998894369549389889437464477988943663531398894370210224374644719889436748095
    #     schedule.run_pending()
    #     time.sleep(50)SZsh@2345678v
