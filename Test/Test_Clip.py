import Clipboard
import time 
# 调用setText设置剪贴板内容
Clipboard.setText("tianyuping")
# 从剪贴板获取刚才设置到剪贴板的内容     
Clipboard.getText()

# 操作需要输入粘贴板内容的标签                                               
getElement(driver,"xpath","//span[text()='点击上传']").click()
time.sleep(1)
# 模拟键盘组合键Ctrl+v将剪贴板的内容复制到搜索输入框中
KeyboardKeys.twoKeys("ctrl","v")
# 模拟enter键来点击确认按钮                                   
KeyboardKeys.onekey("enter")