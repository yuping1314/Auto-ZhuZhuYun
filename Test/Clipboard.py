import win32clipboard as w
import win32con

class Clipboard():
     """
      模拟Windows设置剪贴板
     """
     # 读取剪贴板
     @staticmethod
     def getText():
         # 打开剪贴板
         w.OpenClipboard()
         # 读取剪贴板中的数据
         d = w.GetClipboardData(win32con.CF_UNICODETEXT)
        # 关闭剪贴板
         w.CloseClipboard()
        # 将读取的数据返回，提供给调用者
         return d
 
     # 设置剪贴板内容
     @staticmethod
     def setText(aString):
         # 打开剪贴板
         w.OpenClipboard()
         # 清空剪贴板
         w.EmptyClipboard()
         # 将数据astring写入剪贴板中
         w.SetClipboardData(win32con.CF_UNICODETEXT,'aString')
         # 关闭剪贴板
         #  w.CloseClipboard()


if __name__ == '__main__':
    Clipboard()
    pass
