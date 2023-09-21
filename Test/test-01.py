#coding=utf-8
'''
selenium
'''
from selenium import webdriver as wd
import time
bc=wd.Chrome()
#bc=wd.Ie(executable_path='f:\\IEDriverServer')
bc.get('https://www.sogou.com')
#模拟键盘事件
from selenium.webdriver.common.keys import Keys
query=bc.find_element_by_id('query')
query.send_keys(Keys.F12)#打开开发者模式
time.sleep(3)
#query.send_keys(Keys.F12)#第二次点击 关闭开发者模式
query.send_keys('selenium')
#模拟回车键
#query.send_keys(Keys.RETURN)
query.send_keys(Keys.ENTER)
time.sleep(3)
#模拟键盘复制、粘贴 ctrl+v
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
import win32clipboard as w
import time
import win32api

#写了2个函数 
#读取剪切板
def get_text():
    w.OpenClicpboard()
    d=w.GetClipboardData(win32con.CF_TEXT)
    w.CloseClipboard()
    return d
#设置剪切板内容
def setText(astring):
    w.OpenClicpboard()
    w.EmptyClipboard()
    w.SetClipboardData(win32con.CF_UNICODETEXT,astring)
    w.CloseClipboard()

vk_CODE={'enter':0x0D,'ctrl':0x11,'a':0x41,'v':0x56,'x':0x58}

#键盘按下
def keyDown(keyName):
    win32api.keybd_event(vk_CODE[keyName],0,0,0)
#键盘抬起 
def keyUp(keyName):
    win32api.keybd_event(vk_CODE[keyName],0,win32con.KEYEVENT_KEYUP,0)
content='光荣之路'
setText(content)
getcontent=get_text()

print('剪切板中的内容：',getcontent.decode('gbk'))
bc.find_element_by_id('kw').click()
time.sleep(1)
keyDown('ctrl')
keyDown('v')
#释放ctrl+v
keyUp('v')
keyUp('ctrl')
time.sleep(1)
bc.find_element_by_id('su').click()
time.sleep(3)
#ActionChains模拟键盘复制、粘贴(并发可以使用)
ActionChains(bc).key_down(Keys.CONTROL).send_keys('a').key_up(Keys.CONTROL).perform()
ActionChains(bc).key_down(Keys.CONTROL).send_keys('x').key_up(Keys.CONTROL).perform()
bc.get('http://www.baidu.com')
bc.find_element_by_id('kw').click()
ActionChains(bc).key_down(Keys.CONTROL).send_keys('v').key_up(Keys.CONTROL).perform()
bc.find_element_by_id('su').click()
#鼠标右键
input_box=bc.find_element_by_id('kw')
ActionChains(bc).context_click(input_box).perform()#鼠标右键
set_text('我是谁')
ActionChains(bc).send_keys('p').perform()#粘贴  #这个没有搞定总是输入P
bc.find_element_by_id('stb').click()
#鼠标左键按住、松开
div=bc.find_element_by_id('div1')
a=ActionChains(bc)
a.click_and_hold(div).perform()#按住左键
time.sleep(2)
a.realease(div).perform()#释放鼠标作左键
a.click_and_hold(div).perform()#按住左键
time.sleep(2)
a.realease(div).perform()
