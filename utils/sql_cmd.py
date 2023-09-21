# encoding: utf-8
'''
@file: sql_cmd.py
@author: yuping
@time: 2023/1/8/008 15:02
'''






sql_upload_ytd = ''' 
SELECT 
	DISTINCT
	op.`快递公司`,
	op.`快递单号` 
FROM
	ods_jst_order_paytime AS op
WHERE
	op.`付款日期` >= STR_TO_DATE( CONCAT( DATE_SUB( CURDATE(), INTERVAL 7 DAY ), MAKETIME( 16, 00, 00 )), "%%Y-%%m-%%d %%H:%%i:%%s" ) 
--	AND op.`付款日期` < STR_TO_DATE( CONCAT( DATE_SUB( CURDATE(), INTERVAL 0 DAY ), MAKETIME( 16, 00, 00 )), "%%Y-%%m-%%d %%H:%%i:%%s" ) 

	AND op.`快递单号` IS NOT NULL 
	AND op.快递公司 != "无需物流" 
	AND op.状态 NOT IN ( "被拆分", "被合并", "取消" ) 
        '''
sql_upload_3day = '''
        SELECT 
        DISTINCT
            jeoo.`快递公司` , 
            jeoo.`快递单号`
        FROM
            jst_export_oms_order AS jeoo
            LEFT JOIN ( SELECT l_id, receive,send FROM zz_export_handled_order ) AS zho ON jeoo.`快递单号` = zho.l_id 
        WHERE
            jeoo.`付款日期` >= STR_TO_DATE( CONCAT( DATE_SUB( CURDATE(), INTERVAL 3 DAY ), MAKETIME( 16, 00, 00 )), "%%Y-%%m-%%d %%H:%%i:%%s" ) 
            AND jeoo.`付款日期` < STR_TO_DATE( CONCAT( CURDATE(), MAKETIME( 16, 00, 00 )), "%%Y-%%m-%%d %%H:%%i:%%s" ) 
            AND jeoo.`快递单号` IS NOT NULL 
            AND jeoo.快递公司 != "无需物流" 
            AND jeoo.状态 NOT IN ( "被拆分", "被合并", "取消" )
            AND zho.send IS  NULL 
        '''
sql_upload_7day = '''
        SELECT 
        DISTINCT
            jeoo.`快递公司` , 
            jeoo.`快递单号`
        FROM
            jst_export_oms_order AS jeoo
            LEFT JOIN ( SELECT l_id, receive,send FROM zz_export_handled_order ) AS zho ON jeoo.`快递单号` = zho.l_id 
        WHERE
            jeoo.`付款日期` >= STR_TO_DATE( CONCAT( DATE_SUB( CURDATE(), INTERVAL 7 DAY ), MAKETIME( 16, 00, 00 )), "%%Y-%%m-%%d %%H:%%i:%%s" ) 
            AND jeoo.`付款日期` < STR_TO_DATE( CONCAT( CURDATE(), MAKETIME( 16, 00, 00 )), "%%Y-%%m-%%d %%H:%%i:%%s" ) 
            AND jeoo.`快递单号` IS NOT NULL 
            AND jeoo.快递公司 != "无需物流" 
            AND jeoo.状态 NOT IN ( "被拆分", "被合并", "取消" )
            AND zho.send IS  NULL 
        '''

sql_upload_30day = '''
SELECT 
	DISTINCT
	op.`快递公司`,
	op.`快递单号` 
FROM
	ods_jst_order_paytime AS op
WHERE
	op.`付款日期` >= STR_TO_DATE( CONCAT( DATE_SUB( CURDATE(), INTERVAL 30 DAY ), MAKETIME( 16, 00, 00 )), "%%Y-%%m-%%d %%H:%%i:%%s" ) 
	AND op.`快递单号` IS NOT NULL 
	AND op.快递公司 != "无需物流" 
	AND op.状态 NOT IN ( "被拆分", "被合并", "取消" ) 
        '''

sql_creat_upload='''
CREATE TABLE IF NOT EXISTS ods_zzy_temp_upload   (    
id int UNSIGNED AUTO_INCREMENT  COMMENT 'id',
c_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
u_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
remark varchar(255) NOT NULL DEFAULT '' COMMENT '备注',
status tinyint(1) NOT NULL DEFAULT 0 COMMENT '上传状态，0未上传，1已上传',
lc_company varchar(20) NOT NULL DEFAULT '' COMMENT '快递公司',
l_id varchar(20) NOT NULL DEFAULT '' COMMENT '快递单号',
PRIMARY KEY (  id )  USING BTREE,   
UNIQUE INDEX UK_upload_l_id ( l_id )USING BTREE  
)ENGINE=InnoDB  DEFAULT CHARSET= utf8mb4   COMMENT= '上传猪猪云清单'

'''


sql_creat_lg_order='''
CREATE TABLE IF NOT EXISTS ods_zzy_lg_order (    

id int UNSIGNED AUTO_INCREMENT  COMMENT 'id',
c_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
u_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
remark varchar(255) NOT NULL DEFAULT '' COMMENT '备注',
揽收时间 varchar(100) NOT NULL DEFAULT '' COMMENT '注意：不可用datetime格式，可能为：  疑似无物流，稍后去猪猪快递云查看最终核查结果' ,
最新时间 varchar(100) NOT NULL DEFAULT '' COMMENT '注意：不可用datetime格式，可能为：  疑似无物流，稍后去猪猪快递云查看最终核查结果' ,
查询时间 datetime     ,
快递单号 varchar(20) NOT NULL DEFAULT '' COMMENT '',
快递公司 varchar(20) NOT NULL DEFAULT '' COMMENT '',
时效 varchar(20) NOT NULL DEFAULT '' COMMENT '',
发出至今 varchar(20) NOT NULL DEFAULT '' COMMENT '',
最新至今 varchar(20) NOT NULL DEFAULT '' COMMENT '',
订单编号 varchar(20) NOT NULL DEFAULT '' COMMENT '',
物流状态 varchar(100) NOT NULL DEFAULT '' COMMENT '可能为：  疑似无物流，稍后去猪猪快递云查看最终核查结果',
条数 tinyint NOT NULL DEFAULT 0 COMMENT '',
第1条物流信息 varchar(255) NOT NULL DEFAULT '' COMMENT '',
倒数第2条物流信息 varchar(255) NOT NULL DEFAULT '' COMMENT '',
最后1条物流信息 varchar(255) NOT NULL DEFAULT '' COMMENT '',
完整物流信息 text COMMENT '',
      
      
      
PRIMARY KEY (  id )  USING BTREE,   
UNIQUE INDEX UK_lgorder_lid ( 快递单号 )USING BTREE  
)ENGINE=InnoDB  DEFAULT CHARSET= utf8mb4   COMMENT= '猪猪云物流信息'

'''



sql_lg_warning_order1='''
SELECT
	DISTINCT
	物流公司 AS 快递公司,
	物流单号 AS 快递单号
FROM
	`ods_jst_lg_warning`

WHERE 

u_time >= STR_TO_DATE(CONCAT(DATE(NOW())," 08:48:00"),'%%Y-%%m-%%d %%T' )   
AND 物流公司  !='无需物流'
AND `status` != '已揽收'
AND 物流公司 IS NOT NULL
AND 物流单号 IS NOT NULL
'''

sql_lg_warning_not_receive1 ='''
SELECT
    DISTINCT
	w.物流单号 AS 快递单号,
	
	CASE 
	WHEN p.`快递公司` IS NOT NULL
	THEN p.`快递公司`
	ELSE w.物流公司
	END 快递公司
	
FROM
	`ods_jst_lg_warning` w
	
LEFT JOIN 
				(SELECT
				DISTINCT 
				快递公司,
				快递单号
				FROM 
					ods_jst_order_paytime
				WHERE `付款日期`>  DATE_SUB(DATE(NOW()),INTERVAL 21 DAY)  -- 7天改为21天(保证性能的前提下，扩大一点数据范围)
				AND 快递单号 IS NOT NULL) p
			
		ON w.物流单号 = p.`快递单号`
		
LEFT JOIN ods_zzy_lg_order z ON w.物流单号 = z.`快递单号`
WHERE 

w.u_time >= STR_TO_DATE(CONCAT(DATE(NOW())," 08:48:00"),'%%Y-%%m-%%d %%T' )  
AND w.物流公司  !='无需物流'
AND w.status != '已揽收'
AND w.物流公司 IS NOT NULL
AND w.物流单号 IS NOT NULL
AND z.`物流状态`LIKE ("%%无物流%%")

'''


sql_pre_send_order1 ='''
SELECT 
	DISTINCT 
	快递公司,
	快递单号
FROM 
	ods_jst_order_paytime 
WHERE 

u_time >= STR_TO_DATE(CONCAT(DATE(NOW())," 12:48:00"),'%%Y-%%m-%%d %%T' )  

AND 付款日期 
				BETWEEN STR_TO_DATE(CONCAT(DATE_SUB(DATE(NOW()),INTERVAL 2 DAY)," 16:00:00"),'%%Y-%%m-%%d %%T' )
				AND STR_TO_DATE(CONCAT(DATE_SUB(DATE(NOW()),INTERVAL 2 DAY)," 23:59:59"),'%%Y-%%m-%%d %%T' )
AND 状态 IN ('已发货','发货中')
AND 标签 LIKE '%%预发货%%'
AND 快递公司 !='无需物流'
AND 快递公司 IS NOT NULL
AND 快递单号 IS NOT NULL

UNION 

-- 江苏东台申通，昨天16到今天16所有订单
SELECT 
	DISTINCT 
	p.快递公司,
	p.快递单号
FROM 
	ods_jst_order_paytime p
LEFT JOIN ods_zzy_lg_order z ON p.快递单号 = z.`快递单号`

WHERE 

p.u_time >= STR_TO_DATE(CONCAT(DATE(NOW())," 12:48:00"),'%%Y-%%m-%%d %%T' )  

AND 付款日期 
				BETWEEN STR_TO_DATE(CONCAT(DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)," 16:00:00"),'%%Y-%%m-%%d %%T' )
				AND STR_TO_DATE(CONCAT(DATE_SUB(DATE(NOW()),INTERVAL 0 DAY)," 16:00:00"),'%%Y-%%m-%%d %%T' )
AND 状态 IN ('已发货','发货中')

AND p.快递公司 !='无需物流'
AND p.快递公司 IS NOT NULL
AND p.快递单号 IS NOT NULL
AND  p.快递公司= '江苏申通-森虎'
AND (z.`物流状态`LIKE ("%%无物流%%") OR z.`物流状态` IS NULL)

'''

sql_pre_send_not_receive1 ='''
SELECT 
    DISTINCT
	p.快递公司,
	p.快递单号
FROM 
	ods_jst_order_paytime p
LEFT JOIN ods_zzy_lg_order z ON p.快递单号 = z.`快递单号`
WHERE 

p.u_time >= STR_TO_DATE(CONCAT(DATE(NOW())," 12:48:00"),'%%Y-%%m-%%d %%T' )  

AND p.付款日期 BETWEEN 
									STR_TO_DATE(
												CONCAT(DATE_SUB(DATE(NOW()),INTERVAL 2 DAY)," 16:00:00"),
												'%%Y-%%m-%%d %%T' )
						AND 
									STR_TO_DATE(
												CONCAT(DATE_SUB(DATE(NOW()),INTERVAL 2 DAY)," 23:59:59"),
												'%%Y-%%m-%%d %%T' )

AND p.状态 IN ('已发货','发货中')
AND p.标签 LIKE '%%预发货%%'
AND p.快递公司 !='无需物流'
AND z.`物流状态`LIKE ("%%无物流%%")
AND p.快递公司 IS NOT NULL
AND p.快递单号 IS NOT NULL

UNION 

-- 江苏东台申通，昨天16到今天16所有订单
SELECT 
	DISTINCT 
	p.快递公司,
	p.快递单号
FROM 
	ods_jst_order_paytime p
LEFT JOIN ods_zzy_lg_order z ON p.快递单号 = z.`快递单号`

WHERE 

p.u_time >= STR_TO_DATE(CONCAT(DATE(NOW())," 12:48:00"),'%%Y-%%m-%%d %%T' )  

AND 付款日期 
				BETWEEN STR_TO_DATE(CONCAT(DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)," 16:00:00"),'%%Y-%%m-%%d %%T' )
				AND STR_TO_DATE(CONCAT(DATE_SUB(DATE(NOW()),INTERVAL 0 DAY)," 16:00:00"),'%%Y-%%m-%%d %%T' )
AND 状态 IN ('已发货','发货中')

AND p.快递公司 !='无需物流'
AND p.快递公司 IS NOT NULL
AND p.快递单号 IS NOT NULL
AND  p.快递公司= '江苏申通-森虎'
AND (z.`物流状态`LIKE ("%%无物流%%") OR z.`物流状态` IS NULL)
'''

sql_pre_send_order2 = '''
SELECT 
	DISTINCT 
	快递公司,
	快递单号
FROM 
	ods_jst_order_paytime 
WHERE 

u_time >= STR_TO_DATE(CONCAT(DATE(NOW())," 17:48:00"),'%%Y-%%m-%%d %%T' )  

AND 付款日期 
				BETWEEN STR_TO_DATE(CONCAT(DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)," 00:00:00"),'%%Y-%%m-%%d %%T' )
				AND STR_TO_DATE(CONCAT(DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)," 16:00:00"),'%%Y-%%m-%%d %%T' )
AND 状态 IN ('已发货','发货中')
AND 标签 LIKE '%%预发货%%'
AND 快递公司 !='无需物流'
AND 快递公司 IS NOT NULL
AND 快递单号 IS NOT NULL
UNION 

-- 江苏东台申通，昨天16到今天16所有订单
SELECT 
	DISTINCT 
	p.快递公司,
	p.快递单号
FROM 
	ods_jst_order_paytime p
LEFT JOIN ods_zzy_lg_order z ON p.快递单号 = z.`快递单号`

WHERE 

p.u_time >= STR_TO_DATE(CONCAT(DATE(NOW())," 17:48:00"),'%%Y-%%m-%%d %%T' )  

AND 付款日期 
				BETWEEN STR_TO_DATE(CONCAT(DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)," 16:00:00"),'%%Y-%%m-%%d %%T' )
				AND STR_TO_DATE(CONCAT(DATE_SUB(DATE(NOW()),INTERVAL 0 DAY)," 16:00:00"),'%%Y-%%m-%%d %%T' )
AND 状态 IN ('已发货','发货中')

AND p.快递公司 !='无需物流'
AND p.快递公司 IS NOT NULL
AND p.快递单号 IS NOT NULL
AND  p.快递公司= '江苏申通-森虎'
AND (z.`物流状态`LIKE ("%%无物流%%") OR z.`物流状态` IS NULL)


'''

sql_pre_send_not_receive2 ='''
SELECT 
    DISTINCT
	p.快递公司,
	p.快递单号
FROM 
	ods_jst_order_paytime p
LEFT JOIN ods_zzy_lg_order z ON p.快递单号 = z.`快递单号`
WHERE 

p.u_time >= STR_TO_DATE(CONCAT(DATE(NOW())," 17:48:00"),'%%Y-%%m-%%d %%T' )  

AND p.付款日期 BETWEEN 
									STR_TO_DATE(
												CONCAT(DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)," 00:00:00"),
												'%%Y-%%m-%%d %%T' )
						AND 
									STR_TO_DATE(
												CONCAT(DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)," 16:00:00"),
												'%%Y-%%m-%%d %%T' )

AND p.状态 IN ('已发货','发货中')
AND p.标签 LIKE '%%预发货%%'
AND p.快递公司 !='无需物流'
AND z.`物流状态`LIKE ("%%无物流%%")
AND p.快递公司 IS NOT NULL
AND p.快递单号 IS NOT NULL

UNION 
SELECT 
	DISTINCT 
	p.快递公司,
	p.快递单号
FROM 
	ods_jst_order_paytime p
LEFT JOIN ods_zzy_lg_order z ON p.快递单号 = z.`快递单号`

WHERE 

p.u_time >= STR_TO_DATE(CONCAT(DATE(NOW())," 17:48:00"),'%%Y-%%m-%%d %%T' )  

AND 付款日期 
				BETWEEN STR_TO_DATE(CONCAT(DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)," 16:00:00"),'%%Y-%%m-%%d %%T' )
				AND STR_TO_DATE(CONCAT(DATE_SUB(DATE(NOW()),INTERVAL 0 DAY)," 16:00:00"),'%%Y-%%m-%%d %%T' )
AND 状态 IN ('已发货','发货中')

AND p.快递公司 !='无需物流'
AND p.快递公司 IS NOT NULL
AND p.快递单号 IS NOT NULL
AND  p.快递公司= '江苏申通-森虎'
AND (z.`物流状态`LIKE ("%%无物流%%") OR z.`物流状态` IS NULL)
'''

