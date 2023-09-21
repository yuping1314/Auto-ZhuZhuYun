import win32clipboard
import win32con
win32clipboard.OpenClipboard()
win32clipboard.EmptyClipboard()
win32clipboard.SetClipboardData(win32con.CF_UNICODETEXT,'fgabhab4')
data = win32clipboard.GetClipboardData()
win32clipboard.CloseClipboard()

print(data)