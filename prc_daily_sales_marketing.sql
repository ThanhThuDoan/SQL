CREATE DEFINER=`root`@`%` PROCEDURE `prc_daily_sales_marketing`()
BEGIN

TRUNCATE TABLE data_kiot_viet.daily_sales_marketing;

INSERT INTO data_kiot_viet.daily_sales_marketing
WITH list_date AS (
SELECT date_field,
DATE_FORMAT(date_field,'%Y-%m') month_field
FROM
(
    SELECT
        MAKEDATE(YEAR(NOW()),1) +
        INTERVAL (MONTH(NOW())-1) MONTH +
        INTERVAL daynum DAY date_field
    FROM
    (
        SELECT t*10+u daynum
        FROM
            (SELECT 0 t UNION SELECT 1 UNION SELECT 2 UNION SELECT 3) A,
            (SELECT 0 u UNION SELECT 1 UNION SELECT 2 UNION SELECT 3
            UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7
            UNION SELECT 8 UNION SELECT 9) B
        ORDER BY daynum
    ) daynum
) list_day
WHERE MONTH(date_field) = MONTH(DATE_ADD(CURDATE(), INTERVAL -1 DAY))
),
prd_list AS
(
SELECT DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -1 DAY),'%Y-%m') month_field,
    prd.code productcode,
    prd.fullname productname,
    CASE WHEN prd_ct.categoryname_1 IS NULL THEN
		CASE WHEN (prd.categoryName LIKE 'Q-%' OR prd.categoryName = 'Quà khuyến mại (Q)') THEN 'Quà khuyến mại (Q)'
        WHEN prd.categoryname LIKE 'Đ.QATT%' THEN '7.Thời trang'
        WHEN (prd.categoryname LIKE 'DDCB%' OR  prd.categoryname LIKE 'DDGD%') THEN '3.Đồ dùng'
        WHEN prd.categoryname LIKE 'DAU%' THEN '1.Đồ ăn uống'
		WHEN prd.categoryName  =  '2.Đồ chơi (DC)' THEN '2.Đồ chơi (DC)'
		WHEN (prd.categoryName LIKE 'SUA%' OR prd.categoryName LIKE 'Sữa%') THEN '6.Sữa'
        WHEN prd.categoryName LIKE 'HMP%' THEN '5.Hóa mỹ phẩm'
        WHEN prd.categoryName LIKE 'BIM%' THEN '0.Bỉm'
        ELSE NULL END
	ELSE prd_ct.categoryname_1 END categoryName_1,
    prd.categoryName
FROM
    data_kiot_viet.product prd
        LEFT JOIN
    data_kiot_viet.product_categories prd_ct ON prd.code = prd_ct.code
WHERE
    prd.categoryName NOT IN ('Tài sản công ty (TSCT)' , 'POSM',
        'Error',
        'Vật tư',
        'Voucher',
        'Văn phòng phẩm',
        'Thiết bị IT',
        'Thiết bị Setup - Bảo trì')
)
SELECT list_date.*,
prd_list.productcode,
prd_list.productname,
prd_list.categoryname_1,
prd_list.categoryName,
CASE WHEN mar_dl.productcode IS NULL THEN 'Non-Sales' ELSE 'Sales' END flag_daily,
CASE WHEN mar_dk.productcode IS NULL THEN 'Non-Sales' ELSE 'Sales' END flag_dinhky,
(SELECT DATEDIFF(MAX(createdDate), MIN(createdDate))+1 FROM data_kiot_viet.daily_sales WHERE DATE_FORMAT(createdDate,'%Y-%m') = DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -1 DAY),'%Y-%m')) number_of_days,
IFNULL(ds_cur.quantity,0) quantity,
IFNULL(ds_cur.subtotal,0) subtotal
FROM list_date 
INNER JOIN prd_list
ON list_date.month_field = prd_list.month_field
LEFT JOIN 
(
SELECT createdDate, productcode, SUM(quantity) quantity, SUM(subtotal) subtotal
FROM data_kiot_viet.daily_sales 
WHERE DATE_FORMAT(createdDate,'%Y-%m') = DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -1 DAY),'%Y-%m')
GROUP BY createdDate, productcode
) ds_cur
ON list_date.date_field = ds_cur.createdDate
AND prd_list.productCode = ds_cur.productCode
LEFT JOIN data_kiot_viet.master_marketing_ctkm_daily mar_dl
ON prd_list.productcode = mar_dl.productcode
AND list_date.date_field = STR_TO_DATE(mar_dl.date,'%d/%m/%Y')
LEFT JOIN data_kiot_viet.master_marketing_ctkm_dinhky mar_dk
ON prd_list.productcode = mar_dk.productcode
AND list_date.date_field BETWEEN STR_TO_DATE(mar_dk.fromdate,'%d/%m/%Y') AND STR_TO_DATE(mar_dk.todate,'%d/%m/%Y')

UNION ALL

SELECT prd_prv.month_field,
prd_prv.month_field,
prd_prv.productcode,
prd_prv.productname,
prd_prv.categoryname_1,
prd_prv.categoryname,
'Non-Sales',
'Non-Sales',
prd_prv.number_of_days,
IFNULL(ds_prv.quantity,0) quantity,
IFNULL(ds_prv.subtotal,0) subtotal
FROM (
SELECT DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -1 MONTH),'%Y-%m') month_field,
	(SELECT DATEDIFF(MAX(createdDate), MIN(createdDate))+ 1 FROM data_kiot_viet.daily_sales WHERE DATE_FORMAT(createdDate,'%Y-%m') = DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -1 MONTH),'%Y-%m')) number_of_days,
    prd.code productcode,
    prd.fullname productname,
    CASE WHEN prd_ct.categoryname_1 IS NULL THEN
		CASE WHEN (prd.categoryName LIKE 'Q-%' OR prd.categoryName = 'Quà khuyến mại (Q)') THEN 'Quà khuyến mại (Q)'
        WHEN prd.categoryname LIKE 'Đ.QATT%' THEN '7.Thời trang'
        WHEN (prd.categoryname LIKE 'DDCB%' OR  prd.categoryname LIKE 'DDGD%') THEN '3.Đồ dùng'
        WHEN prd.categoryname LIKE 'DAU%' THEN '1.Đồ ăn uống'
		WHEN prd.categoryName  =  '2.Đồ chơi (DC)' THEN '2.Đồ chơi (DC)'
		WHEN (prd.categoryName LIKE 'SUA%' OR prd.categoryName LIKE 'Sữa%') THEN '6.Sữa'
        WHEN prd.categoryName LIKE 'HMP%' THEN '5.Hóa mỹ phẩm'
        WHEN prd.categoryName LIKE 'BIM%' THEN '0.Bỉm'
        ELSE NULL END
	ELSE prd_ct.categoryname_1 END categoryName_1,
    prd.categoryName
FROM
    data_kiot_viet.product prd
        LEFT JOIN
    data_kiot_viet.product_categories prd_ct ON prd.code = prd_ct.code
WHERE
    prd.categoryName NOT IN ('Tài sản công ty (TSCT)' ,'POSM',
        'Error',
        'Vật tư',
        'Voucher',
        'Văn phòng phẩm',
        'Thiết bị IT',
        'Thiết bị Setup - Bảo trì')
UNION ALL 
SELECT DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -2 MONTH),'%Y-%m') month_field,
	(SELECT DATEDIFF(MAX(createdDate), MIN(createdDate))+ 1 FROM data_kiot_viet.daily_sales WHERE DATE_FORMAT(createdDate,'%Y-%m') = DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -2 MONTH),'%Y-%m')) number_of_days,
    prd.code productcode,
    prd.fullname productname,
    CASE WHEN prd_ct.categoryname_1 IS NULL THEN
		CASE WHEN (prd.categoryName LIKE 'Q-%' OR prd.categoryName = 'Quà khuyến mại (Q)') THEN 'Quà khuyến mại (Q)'
        WHEN prd.categoryname LIKE 'Đ.QATT%' THEN '7.Thời trang'
        WHEN (prd.categoryname LIKE 'DDCB%' OR  prd.categoryname LIKE 'DDGD%') THEN '3.Đồ dùng'
        WHEN prd.categoryname LIKE 'DAU%' THEN '1.Đồ ăn uống'
		WHEN prd.categoryName  =  '2.Đồ chơi (DC)' THEN '2.Đồ chơi (DC)'
		WHEN (prd.categoryName LIKE 'SUA%' OR prd.categoryName LIKE 'Sữa%') THEN '6.Sữa'
        WHEN prd.categoryName LIKE 'HMP%' THEN '5.Hóa mỹ phẩm'
        WHEN prd.categoryName LIKE 'BIM%' THEN '0.Bỉm'
        ELSE NULL END
	ELSE prd_ct.categoryname_1 END categoryName_1,
    prd.categoryName
FROM
    data_kiot_viet.product prd
        LEFT JOIN
    data_kiot_viet.product_categories prd_ct ON prd.code = prd_ct.code
WHERE
    prd.categoryName NOT IN ('Tài sản công ty (TSCT)' , 'POSM',
        'Error',
        'Vật tư',
        'Voucher',
        'Văn phòng phẩm',
        'Thiết bị IT',
        'Thiết bị Setup - Bảo trì')
        ) prd_prv
LEFT JOIN
(
SELECT 
    DATE_FORMAT(createdDate, '%Y-%m') createdDate,
    DATE_FORMAT(createdDate, '%Y-%m') m_date,
    productcode,
    SUM(quantity) quantity,
    SUM(subtotal) subtotal
FROM
    data_kiot_viet.daily_sales
WHERE
    DATE_FORMAT(createdDate,'%Y-%m') IN (DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -2 MONTH),'%Y-%m'),DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -1 MONTH),'%Y-%m'))
GROUP BY DATE_FORMAT(createdDate, '%Y-%m') , productcode
) ds_prv
ON prd_prv.productcode = ds_prv.productcode
AND prd_prv.month_field = ds_prv.m_date;

COMMIT;

END