# Auto_ZhuZhuYun    
    
## 功能      
* 读取文件夹表格,然后使用猪猪云查询物流信息      
* 读取数据库订单,然后使用猪猪云查询物流信息      
* 预揽收收前，使用猪猪云查询物流信息      
    
## 结构树  
E:\SENHU\PYCHARM    
Auto_ZhuZhuYun    
    │  start_zhuzhuyun.py  *主程序*      
    ├─config  *配置*    
    │      config.ini  #数据库及路径等     
    │      config.yaml  #快递公司及猪猪云账号密码         
    ├─log  #日志  
    ├─prepare_data  *上传数据准备*    
    │  │  cleaning.py  #数据清洗  
    │  │  get.py  #获取数据  
    ├─query_data  *上传+下载*     
    │  └─ zzy.py  #程序整合    
    └─utils  *工具箱*    
        │  dbmanager.py  #数据库    
        │  logger.py  #日志      
        │  read_config.py  #读取配置类     
        │  sql_cmd.py  #sql语句     
  
## 运行方法  
  
自动运行：    
* start_zhuzhuyun-运行      
  
手动运行：    
1. 选择对应的job取消注释    
   > job1: 读取文件夹数据      
   > job2: 传入sql语句，查询不同范围的订单数据    
   > job3: 9点未揽收(物流预警订单)      
   > job4: 13点,18未揽收(预发货订单)  
2. start_zhuzhuyun-运行       
  
  
## 思路      
* 读取数据：文件或数据库      
    > 处理为两列——快递公司+快递单号        
    > df进行清洗操作为标准快递公司名和编号       
    > 写入数据库，字段  status=0 ，remark=''  **注意：** 每次读取后状态都会变为未上传状态      
* 上传数据     
    > 获取上传数据，WHERE status=0 + LIMIT 10000,默认取更新日期为当天，如果其它日期也没有上传，则可以改参数    
    > 上传成功,更新status=1  ,猪猪云remark更新为文件名      
  
* 下载    
    > 获取数据库当天上传文件的文件名进行遍例下载      
    > 如果是查询的文件，则另外导出一份合并文件，**注意：**目前导出的格式为xlsx如果数据量大，要分表    
* 预发货处理    
    > 读取下载的数据    
    > 清洗并按快递公司输出需要预揽收的订单    
      
    
## 注意事项    
* 注意猪猪云上传数据量尽量剔除无需查询订单，避免网站崩溃     
* 新增快递公司需要更改配置文件config.yaml    

