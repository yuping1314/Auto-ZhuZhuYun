# encoding: utf-8
'''
@file: zzy.py
@author: yuping
@time: 2023/7/10/010 13:23
'''

import random, time, requests
import os, shutil
from datetime import datetime, timedelta
import hashlib
import pandas as pd
import warnings

warnings.filterwarnings('ignore')
from utils.dbmanager import DBManager

# from utility_class.spider.useragent import user_agent_pc

from utils.read_config import ReadConfig
import logging
from utils.logger import logger

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

user_agent_pc = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'


class ZZY(object):
    def __init__(self, upload_start_time):
        self.logger = logger()
        self.config_ini = ReadConfig().conf_ini()
        self.config_yaml = ReadConfig().conf_yaml()

        self.db = DBManager(self.logger, self.config_ini, choose_db='db')

        self.url = 'http://yun.zhuzhufanli.com/'
        self.login_url = r'http://yun.zhuzhufanli.com/Login.aspx'
        self.tasklist_url = r'http://yun.zhuzhufanli.com/TaskList.aspx'
        self.addnewtask_url = r'http://yun.zhuzhufanli.com/AddNewTask.aspx'

        self.username = self.config_yaml['login']['username']
        self.userpwd = self.config_yaml['login']['userpwd']

        self.upload_start_time = upload_start_time

    def _get_data(self, upload_data_start, upload_data_end, lc_company):

        sql_cmd = f'''
        SELECT
            * 
        FROM
            ods_zzy_temp_upload 
        WHERE
        u_time BETWEEN 
        STR_TO_DATE( CONCAT(DATE_SUB(CURDATE(),INTERVAL {upload_data_start} DAY ), ' ', '00:00:00' ), '%%Y-%%m-%%d %%H:%%i:%%s' ) 
        AND 
        STR_TO_DATE(CONCAT(DATE_SUB(CURDATE(),INTERVAL {upload_data_end} DAY ), ' ', '23:59:59' ),'%%Y-%%m-%%d %%H:%%i:%%s')
        AND status=0  
        AND lc_company='{lc_company}'      
        LIMIT 4999 
        '''
        df = self.db.pandas_read_db(sql_cmd)
        return df

    def start(self, source_type):
        lc_map = self.config_yaml['lc_map']
        upload_data_start = self.config_ini.getint('get_data', 'upload_data_start')
        upload_data_end = self.config_ini.getint('get_data', 'upload_data_end')
        download_data_start = self.config_ini.getint('get_data', 'download_data_start')
        download_data_end = self.config_ini.getint('get_data', 'download_data_end')

        session = ZZYLogin(self.upload_start_time).login()
        upload = ZZYUpload(self.upload_start_time)
        for lc_company, lc_company_en in lc_map.items():
            self.index = 1
            for index in range(1, 10000):
                self.logger.debug(('start_index', index))
                df = self._get_data(upload_data_start, upload_data_end, lc_company)
                if df.empty:
                    self.logger.info(f'未获取到{lc_company}快递单号~~~')
                    break
                else:
                    self.logger.info(f'共获取到{lc_company}{(df.shape)[0]}单~~~')
                    upload.upload(session, df, lc_company, lc_company_en, index)

        dl = ZZYDownload(self.upload_start_time)
        dl.backups()
        download_list = dl.get_download_list(download_data_start, download_data_end)

        dl.zzy_download(download_list)
        dl.readfile_and_savedb(source_type)


class ZZYLogin(ZZY):
    def __init__(self, upload_start_time):
        super(ZZYLogin, self).__init__(upload_start_time)

    def login(self):
        headers = {
            # 'User_Agent': random.choice(user_agent_pc),
            'User_Agent': user_agent_pc,
            'Accept': 'application/json,text/javascript,*/*;q=0.01',
            'Accept-Encoding': 'gzip,deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Length': '60',
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',

            'Host': 'yun.zhuzhufanli.com',

            'X-Requested-With': 'XMLHttpRequest'
        }

        data = {'username': self.username, 'userpwd': self.userpwd}
        session = requests.Session()
        resp = session.post(url=self.login_url, headers=headers, data=data)
        self.logger.info(('登陆请求结果：', resp.text))
        time.sleep(1)
        return session


class ZZYUpload(ZZY):
    def __init__(self, upload_start_time):
        super(ZZYUpload, self).__init__(upload_start_time)
        self.index = 1

    def _concat_upload_data(self, lc_company, lc_company_en, order_list, index):

        today_str = datetime.strftime(datetime.today(), '%Y%m%d')
        import uuid
        uk_id = uuid.uuid1()
        file_name = today_str + '_' + lc_company + '_' + str(index) + '_' + str(uk_id)
        order_list_str = '\n'.join(order_list)

        dt_stamp = str(int(datetime.timestamp(datetime.now()) * 1000))
        hash_str = order_list_str + "i4jhef3CBuyd5ZVlADFA" + dt_stamp
        hash2242 = hashlib.md5(hash_str.encode(encoding='UTF-8')).hexdigest()

        upload_data = {
            'kdgs': lc_company_en,
            'kddhs': order_list_str,

            'zffs': 'baoyue',
            'note': file_name,
            'hash2242': hash2242 + '|' + dt_stamp
        }

        return upload_data, file_name

    def _post(self, session, df, upload_data, file_name):
        # resp = session.post(self.addnewtask_url, data=upload_data)
        #
        # resp = resp.json()
        #
        # if resp['code'] == 1:
        #     self._update_status(df, file_name)
        #     self.logger.info(f'上传成功！等待30s后继续操作~~~')
        #     time.sleep(30)
        #     self.index += 1
        #
        # else:
        #     self.logger.info(('上传后,resp返回值：', resp))
        #
        #     time.sleep(120)
        url = 'http://yun.zhuzhufanli.com/'
        login_url = r'http://yun.zhuzhufanli.com/Login.aspx'
        tasklist_url = r'http://yun.zhuzhufanli.com/TaskList.aspx'
        addnewtask_url = r'http://yun.zhuzhufanli.com/AddNewTask.aspx'
        b = webdriver.Chrome()
        b.get(url=url)
        time.sleep(5)
        b.find_element_by_xpath('//*[@id="login_link_btn"]').click()
        b.switch_to.frame('layui-layer-iframe1')
        b.find_element_by_xpath('/html/body/form[1]/div[1]/div[1]/input').send_keys('江苏森虎')
        b.find_element_by_xpath('/html/body/form[1]/div[2]/div[1]/input').send_keys('888888')
        b.find_element_by_xpath('/html/body/form[1]/div[3]/div/button').click()
        time.sleep(3)
        # b.find_element_by_xpath('/html/body/div[2]/div[1]/div/p[2]/button[1]').click()
        # time.sleep(2)
        b.get('http://yun.zhuzhufanli.com/AddNewTask.aspx')
        time.sleep(2)
        # b.find_element_by_xpath('/html/body/form[1]/div[1]/div[1]/div/div/input').send_keys('极兔')

        b.find_element_by_xpath('/html/body/form[1]/div[1]/div[1]/div/div/i').click()
        b.find_element_by_xpath('/html/body/form[1]/div[1]/div[1]/div/dl/dd[@lay-value="ewe"]').click()
        b.find_element_by_xpath('/html/body/form[1]/div[4]/div/textarea').send_keys(
            '773184673325334\n773184620431586\n')
        b.find_element_by_xpath('/html/body/form[1]/div[6]/div/div[2]/i').click()
        b.find_element_by_xpath('/html/body/form[1]/div[10]/div/button').click()
        # print(b.current_url)
        time.sleep(3)

    def _update_status(self, df, file_name):

        df['status'] = 1
        df['remark'] = file_name
        update_key = ['status', 'remark']
        table_name = 'ods_zzy_temp_upload'
        self.db.executemany(df=df, table_name=table_name, execute_type='on duplicate key update', update_key=update_key)

    def upload(self, session, df, lc_company, lc_company_en, index):
        if index == 1:
            self.index = 1
        self.logger.debug(('upload_index', self.index))
        order_list = df['l_id'].to_list()
        upload_data, file_name = self._concat_upload_data(lc_company, lc_company_en, order_list, self.index)
        self.logger.info(f'正在准备上传数据{file_name}~~~')

        t = 0
        n = 0
        while True:
            n += 2
            if t <= 6:
                try:
                    self._post(session, df, upload_data, file_name)
                    break
                except:
                    self.logger.error(f'猪猪云上传失败', exc_info=True)
                    time.sleep(120)
                    session = ZZYLogin(self.upload_start_time).login()

                    t += 2
                finally:
                    time.sleep(random.uniform(6, 10))
            else:
                self.logger.info(f'{file_name}上传三次，均失败，请检查！')
                break


class ZZYDownload(ZZY):
    def __init__(self, upload_start_time):
        super(ZZYDownload, self).__init__(upload_start_time)
        self.db = DBManager(self.logger, self.config_ini, choose_db='db')

        self.queried_path = self.config_ini.get('path', 'queried')
        self.backups_path = self.config_ini.get('path', 'backups')
        self.concat_path = self.config_ini.get('path', 'concat')

    def backups(self):

        self.logger.info('将已经下载的文件进行备份~')

        for i in os.listdir(self.queried_path):
            f_path = self.queried_path + '\\' + i
            f_new_path = self.backups_path + '\\' + i
            shutil.move(f_path, f_new_path)

    def get_download_list(self, download_data_start, download_data_end):
        self.logger.debug(self.upload_start_time)
        sql_cmd = f'''
                SELECT
                    DISTINCT remark 
                FROM
                    ods_zzy_temp_upload 
                WHERE 
                remark!=''
                AND  
                u_time BETWEEN 
                -- 默认0，当天查询的数据，如果有特殊情况，如昨天的数据未成功下载，可以改参数进行下载
                STR_TO_DATE( CONCAT(DATE_SUB(CURDATE(),INTERVAL {download_data_start} DAY ), ' ', '00:00:00' ), '%%Y-%%m-%%d %%H:%%i:%%s' ) 
                AND 
                STR_TO_DATE(CONCAT(DATE_SUB(CURDATE(),INTERVAL {download_data_end} DAY ), ' ', '23:59:59' ),'%%Y-%%m-%%d %%H:%%i:%%s')
                AND u_time>'{self.upload_start_time}' 
                '''

        download_list = list(self.db.pandas_read_db(sql_cmd)['remark'])
        self.logger.info(('下载文件目录：', download_list))
        return download_list

    def zzy_download(self, download_list):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument("--start-maximized")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ['enable-automation'])
        prefs = {
            'download.default_directory': self.queried_path,
        }
        options.add_experimental_option("prefs", prefs)

        driver = webdriver.Chrome(options=options)
        driver.implicitly_wait(5)

        with open(r'E:\py\senhu\stealth.min.js', 'r') as fp:
            js = fp.read()
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument", {
                "source": js})

        driver.get(url=self.url)
        time.sleep(10)  # 网站加载问题，导致无法点击登陆，必须强制等待(显示等待无效)

        try:
            WebDriverWait(
                driver, 10, 0.5).until(
                EC.element_to_be_clickable(
                    (By.ID, 'login_link_btn')))
            login_btn = driver.find_element_by_id('login_link_btn')
            time.sleep(random.uniform(3, 6))
            # login_btn.click() #偶尔会失效，更换点击方法
            driver.execute_script("arguments[0].click()", login_btn)
            self.logger.info('点击登陆按钮')
            time.sleep(3)
        except:
            self.logger.error('未定位到登陆按钮', exc_info=True)
        driver.switch_to.frame('layui-layer-iframe1')
        time.sleep(random.uniform(3, 6))
        self.logger.info('猪猪云登陆中~~~')
        username_input = driver.find_element_by_xpath('//div/input[@name="username"]')
        userpwd_input = driver.find_element_by_xpath('//div/input[@name="userpwd"]')
        login_btn_enter = driver.find_element_by_xpath('//div[@class="layui-input-block"]/button')

        ActionChains(driver).click(username_input).send_keys(self.username).perform()
        time.sleep(random.uniform(3, 6))
        ActionChains(driver).click(userpwd_input).send_keys(self.userpwd).perform()
        time.sleep(random.uniform(3, 6))
        ActionChains(driver).click(login_btn_enter).perform()
        time.sleep(5)
        self.logger.info('猪猪云已成功登陆~~~')

        headers = {
            'User_Agent': user_agent_pc}
        s = requests.Session()
        data = {
            'username': self.username,
            'userpwd': self.userpwd
        }
        s.post(url=self.login_url, headers=headers, data=data)
        time.sleep(10)

        for file_name in download_list:

            usenow_btn = driver.find_element_by_xpath(
                '/html/body/div[2]/div[1]/div/p[2]/button[1]')
            usenow_btn.click()
            time.sleep(random.uniform(3, 6))
            driver.switch_to.frame('layui-layer-iframe1')
            time.sleep(random.uniform(3, 6))
            self.logger.info('准备下载{}~~~'.format(file_name))
            n = 0
            t = 0
            while True:
                if n <= 10:
                    try:
                        time.sleep(random.uniform(3, 6))
                        result_btn = driver.find_elements_by_xpath(
                            '//span[@lay-tip="{}"]/parent::*/parent::*/td[7]/a[1]'.format(file_name))[
                            0]
                        result_btn.click()
                        time.sleep(random.uniform(3, 6))
                        try:
                            driver.switch_to.default_content()
                            time.sleep(10)
                            driver.switch_to.frame('layui-layer-iframe2')
                            time.sleep(5)
                            href_url = driver.find_element_by_xpath(
                                '/html/body/div[1]/div/div[1]/div/a[4]').get_attribute('href')
                            driver.refresh()

                            download_content = s.get(href_url).content
                            with open('{path}\{file_name}.xlsx'.format(path=self.queried_path, file_name=file_name),
                                      'wb') as fp:
                                fp.write(download_content)
                            self.logger.info('{} 下载成功'.format(file_name))
                            time.sleep(2)
                            break
                        except:
                            n = 0
                            t += 1
                            if t <= 180:
                                self.logger.info('无数据,一分钟后再次查询，已经查询耗时{}分钟'.format(t))
                                time.sleep(60)
                                driver.refresh()

                                usenow_btn = driver.find_element_by_xpath(
                                    '/html/body/div[2]/div[1]/div/p[2]/button[1]')
                                usenow_btn.click()
                                time.sleep(random.uniform(3, 6))
                                driver.switch_to.frame('layui-layer-iframe1')
                                time.sleep(random.uniform(3, 6))
                                self.logger.info('准备下载{}，第{}次尝试~~~'.format(file_name, t))
                            else:
                                self.logger.error('60分钟内未成功获取到数据，请检查', exc_info=True)
                                break

                    except:
                        self.logger.info('第{}页没有数据，正准备获取下一页~~~'.format(str(n)))
                        n += 1
                        next_page_btn = driver.find_element_by_xpath('//a[@data-page="{}"]'.format(str(1 + n)))
                        time.sleep(5)
                        next_page_btn.click()
                        time.sleep(random.uniform(3, 6))

                else:
                    n = 0
                    t += 1
                    if t <= 180:
                        self.logger.warn(f'前10页没有数据,一分钟后再次查询', exc_info=True)

                        first_page_btn = driver.find_element_by_xpath('//a[@data-page="1"]')
                        time.sleep(5)
                        first_page_btn.click()

                        time.sleep(60)
                    else:
                        self.logger.error('60分钟内未成功获取到数据，请检查', exc_info=True)
                        break

    def readfile_and_savedb(self, source_type):
        self.logger.info('正在读取文件夹~~~')

        path_lists = []
        for root, dirs, files in os.walk(self.queried_path):
            for file in files:
                file_path = os.path.join(root, file)
                path_lists.append(file_path)

        table_lists = []
        for p in path_lists:
            if p.endswith('.xlsx'):
                df = pd.read_excel(p, engine='openpyxl')
                table_lists.append(df)
                self.logger.info('文件读取完毕！文件列表{}'.format(p))
        if len(table_lists):
            df = pd.concat(table_lists)
            dt = datetime.today().strftime('%Y%m%d')

            self.logger.info('正在写入数据库~~~')

            df = df.apply(
                lambda x: x.fillna(0) if x.dtype.kind in 'fi' else x.fillna('') if x.dtype.kind in 'O' else x)

            df[['查询时间']] = df[['查询时间']].astype('datetime64[ns]')

            update_key = ['u_time', 'remark']

            update_key.extend(df.columns)
            table_name = 'ods_zzy_lg_order'
            self.db.executemany(df=df, table_name=table_name, execute_type='on duplicate key update',
                                update_key=update_key)
            self.logger.info('数据库存储完毕！！！')
            if source_type == 'folder':
                concat_file_path = self.concat_path + '\\' + dt + '.xlsx'
                df.to_excel(concat_file_path, index=False)
                self.logger.info(f'正在存储文件{concat_file_path}~~~')
                df['快递单号'] = df['快递单号'].astype('str')

                self.logger.info(f'文件{concat_file_path}存储完毕！！！')
