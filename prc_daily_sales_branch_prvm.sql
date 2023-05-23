CREATE DEFINER=`root`@`%` PROCEDURE `prc_daily_sales_branch_prvm`()
BEGIN
/*--------------------------------------------------!
01. Get data mục tiêu ngày/tuần/tháng
!--------------------------------------------------*/

SET SQL_MODE = 'NO_ENGINE_SUBSTITUTION';


TRUNCATE TABLE data_kiot_viet.master_sales_target_prvm;
INSERT INTO data_kiot_viet.master_sales_target_prvm

WITH list_date AS (
SELECT date_field, 
DATE_FORMAT(date_field,'%Y-%m') month_field,
COUNT(date_field) OVER(PARTITION BY DATE_FORMAT(date_field,'%Y-%m')) num_day_of_month,
CONCAT('Tuần ',
            FLOOR((DAYOFMONTH(date_field) - 1) / 7) + 1) week_field,
COUNT(date_field) OVER(PARTITION BY CONCAT('Tuần ',
            FLOOR((DAYOFMONTH(date_field) - 1) / 7) + 1)) num_day_of_week
FROM
(
    SELECT
        MAKEDATE(YEAR(NOW()),1) +
        INTERVAL (MONTH(NOW())-3) MONTH +
        INTERVAL daynum DAY date_field
    FROM
    (
        SELECT t*11+u daynum
        FROM
            (SELECT 0 t UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) A,
            (SELECT 0 u UNION SELECT 1 UNION SELECT 2 UNION SELECT 3
            UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7
            UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) B
        ORDER BY daynum
    ) daynum
) list_day
WHERE MONTH(date_field) IN (MONTH(NOW() - INTERVAL 2 MONTH),MONTH(NOW() - INTERVAL 1 MONTH))
    AND #(
          YEAR(date_field) = YEAR(NOW()) 
        #OR 
          #YEAR(date_field) = YEAR(NOW() - INTERVAL 2 MONTH))
AND NOT EXISTS (SELECT NULL FROM data_kiot_viet.data_holiday_bbs hld WHERE list_day.date_field = hld.date_field
AND list_day.date_field BETWEEN '2023-01-20' AND '2023-01-24')
)
SELECT list_date.*, target.cua_hang,
target.ma_nv_asm,
target.muc_tieu muc_tieu_thang,
ROUND(target.muc_tieu*list_date.num_day_of_week/list_date.num_day_of_month,0) muc_tieu_tuan,
ROUND(target.muc_tieu/list_date.num_day_of_month,0) muc_tieu_ngay,
ROUND(target.muc_tieu/list_date.num_day_of_month,0) muc_tieu_ngay_bk,
target.aov
FROM list_date
LEFT JOIN data_kiot_viet.master_sales_target target
ON list_date.month_field = target.thang
;

COMMIT;

TRUNCATE TABLE data_kiot_viet.daily_sales_branch_prvm;

INSERT INTO data_kiot_viet.daily_sales_branch_prvm
-- Invoice - Customer code null:
WITH nocust_inv_rs AS (
SELECT createdDate, 
m_rp, 
y_rp, 
branchId, 
branchName,
invoiceId,
code,
customercode,
sales_code,
COUNT(DISTINCT productcode) Slg_SKU,
SUM(quantity) quantity,
SUM(subtotal) subtotal,
SUM(subtotal_f) subtotal_f
FROM (
SELECT 
    STR_TO_DATE(inv_dt.createdDate, '%Y-%m-%d') createdDate,
    DATE_FORMAT(STR_TO_DATE(inv_dt.createdDate, '%Y-%m-%d'),'%Y-%m') m_rp,
    DATE_FORMAT(STR_TO_DATE(inv_dt.createdDate, '%Y-%m-%d'),'%Y') y_rp,
    inv_dt.code,
    inv.customerCode,
    inv_dt.invoiceid,
    inv_dt.productcode,
    inv_dt.productname,
    inv.branchId,
    inv_dt.branchname,
    inv_dt.quantity,
    inv_dt.subtotal,
    CASE WHEN (SUM(inv_dt.subtotal) OVER(PARTITION BY inv_dt.code) - inv_dt.total) > 0
		THEN  ROUND(inv_dt.subtotal - (SUM(inv_dt.subtotal) OVER(PARTITION BY inv_dt.code) - inv_dt.total)*inv_dt.subtotal/SUM(inv_dt.subtotal) OVER(PARTITION BY inv_dt.code),0)
		ELSE inv_dt.subtotal END subtotal_f,
    IFNULL(CASE
        WHEN
            MID(inv_dt.soldByName,
                REGEXP_INSTR(inv_dt.soldByName, 'NV[0-9]', 2),
                6) = ''
        THEN
            NULL
        ELSE MID(inv_dt.soldByName,
            REGEXP_INSTR(inv_dt.soldByName, 'NV[0-9]', 2),
            6)
    END,'Undefined') sales_code
FROM
    data_kiot_viet.invoice_details_2022 inv_dt INNER JOIN 
    data_kiot_viet.invoices_2022 inv
    ON  inv_dt.invoiceid = inv.id
WHERE DATE(inv_dt.createdDate) BETWEEN '2023-01-01' AND '2023-01-31'
AND inv.status <> 2
) rs
WHERE customercode = ''
GROUP BY createdDate, 
m_rp, 
y_rp, 
branchId, 
branchName,
invoiceId,
code,
customercode,
sales_code
)
SELECT createdDate,
m_rp,
y_rp,
branchId,
branchName,
sales_code,
COUNT(DISTINCT code) Slg_HD,
COUNT(DISTINCT code) Slg_KH,
SUM(Slg_SKU) Slg_SKU,
SUM(quantity) quantity,
SUM(subtotal) subtotal,
SUM(subtotal_f) subtotal_f
FROM nocust_inv_rs
GROUP BY createdDate,
m_rp,
y_rp,
branchId,
branchName,
sales_code;
COMMIT;

-- Invoice - Customer code not null:
INSERT INTO data_kiot_viet.daily_sales_branch_prvm
WITH cust_inv_rs AS (
SELECT createdDate, 
m_rp, 
y_rp, 
branchId, 
branchName,
invoiceId,
code,
customercode,
sales_code,
COUNT(DISTINCT productcode) Slg_SKU,
SUM(quantity) quantity,
SUM(subtotal) subtotal,
SUM(subtotal_f) subtotal_f
FROM (
SELECT 
    STR_TO_DATE(inv_dt.createdDate, '%Y-%m-%d') createdDate,
    DATE_FORMAT(STR_TO_DATE(inv_dt.createdDate, '%Y-%m-%d'),'%Y-%m') m_rp,
    DATE_FORMAT(STR_TO_DATE(inv_dt.createdDate, '%Y-%m-%d'),'%Y') y_rp,
    inv_dt.code,
    inv.customerCode,
    inv_dt.invoiceid,
    inv_dt.productcode,
    inv_dt.productname,
    inv.branchId,
    inv_dt.branchname,
    inv_dt.quantity,
    inv_dt.subtotal,
    CASE WHEN (SUM(inv_dt.subtotal) OVER(PARTITION BY inv_dt.code) - inv_dt.total) > 0
		THEN  ROUND(inv_dt.subtotal - (SUM(inv_dt.subtotal) OVER(PARTITION BY inv_dt.code) - inv_dt.total)*inv_dt.subtotal/SUM(inv_dt.subtotal) OVER(PARTITION BY inv_dt.code),0)
		ELSE inv_dt.subtotal END subtotal_f,
    IFNULL(CASE
        WHEN
            MID(inv_dt.soldByName,
                REGEXP_INSTR(inv_dt.soldByName, 'NV[0-9]', 2),
                6) = ''
        THEN
            NULL
        ELSE MID(inv_dt.soldByName,
            REGEXP_INSTR(inv_dt.soldByName, 'NV[0-9]', 2),
            6)
    END,'Undefined') sales_code
FROM
    data_kiot_viet.invoice_details_2022 inv_dt INNER JOIN 
    data_kiot_viet.invoices_2022 inv
    ON  inv_dt.invoiceid = inv.id
WHERE DATE(inv_dt.createdDate) BETWEEN '2023-01-01' AND '2023-01-31'
AND inv.status <> 2
) rs
WHERE customerCode <> ''
GROUP BY createdDate, 
m_rp, 
y_rp, 
branchId, 
branchName,
invoiceId,
code,
customercode,
sales_code
)
SELECT createdDate,
m_rp,
y_rp,
branchId,
branchName,
sales_code,
COUNT(DISTINCT code) Slg_HD,
COUNT(DISTINCT customercode) Slg_KH,
SUM(Slg_SKU) Slg_SKU,
SUM(quantity) quantity,
SUM(subtotal) subtotal,
SUM(subtotal_f) subtotal_f
FROM cust_inv_rs
GROUP BY createdDate,
m_rp,
y_rp,
branchId,
branchName,
sales_code;
COMMIT;

-- Return - Invoice ID Null - Customer code not null:
INSERT INTO data_kiot_viet.daily_sales_branch_prvm
WITH noinv_cust_rt AS (
SELECT createdDate,
m_rp,
y_rp,
branchID,
branchName,
invoiceId,
code,
customerCode,
sales_code,
COUNT(DISTINCT productcode) Slg_SKU,
SUM(quantity) quantity,
SUM(subtotal) subtotal,
SUM(subtotal_f) subtotal_f
FROM (
SELECT 
    STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d') createdDate,
    DATE_FORMAT(STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d'),
            '%Y-%m') m_rp,
    DATE_FORMAT(STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d'),
            '%Y') y_rp,
	rt.invoiceId,
	rt_dt.code,
    rt.customerCode,
    rt_dt.productcode,
    rt_dt.productname,
    rt.branchId,
    rt_dt.branchname,
    IF(rt_dt.quantity=0,0,rt_dt.quantity*-1) quantity,
    IF((rt_dt.quantity * rt_dt.price)=0,0,(rt_dt.quantity * rt_dt.price) * - 1) AS subtotal,
    IF((rt_dt.quantity * rt_dt.price)=0,0,(rt_dt.quantity * rt_dt.price) * - 1) AS subtotal_f,
    IFNULL(CASE
                WHEN
                    MID(rt_dt.soldByName,
                        REGEXP_INSTR(rt_dt.soldByName, 'NV[0-9]', 2),
                        6) = ''
                THEN
                    NULL
                ELSE MID(rt_dt.soldByName,
                    REGEXP_INSTR(rt_dt.soldByName, 'NV[0-9]', 2),
                    6)
            END,
            'Undefined') sales_code
FROM
    data_kiot_viet.returns_details_2022 rt_dt
        INNER JOIN
    data_kiot_viet.returns_2022 rt ON rt_dt.code = rt.code
WHERE
    rt.status = 1
        AND DATE(rt_dt.createdDate) BETWEEN '2023-01-01' AND '2023-01-31'
        AND invoiceId IS NULL
) rs_rt
GROUP BY createdDate,
m_rp,
y_rp,
branchID,
branchName,
invoiceId,
code,
customerCode,
sales_code
)
SELECT createdDate,
m_rp,
y_rp,
branchId,
branchName,
sales_code,
COUNT(DISTINCT code)*-1 Slg_HD,
COUNT(DISTINCT customercode)*-1 Slg_KH,
SUM(Slg_SKU)*-1 Slg_SKU,
SUM(quantity) quantity,
SUM(subtotal) subtotal,
SUM(subtotal_f) subtotal_f
FROM noinv_cust_rt
WHERE customerCode IS NOT NULL
GROUP BY createdDate,
m_rp,
y_rp,
branchId,
branchName,
sales_code;
COMMIT;

-- Return - Invoice ID Null - Customer code null:
INSERT INTO data_kiot_viet.daily_sales_branch_prvm
WITH noinv_nocust_rt AS (
SELECT createdDate,
m_rp,
y_rp,
branchID,
branchName,
invoiceId,
code,
customerCode,
sales_code,
COUNT(DISTINCT productcode) Slg_SKU,
SUM(quantity) quantity,
SUM(subtotal) subtotal,
SUM(subtotal_f) subtotal_f
FROM (
SELECT 
    STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d') createdDate,
    DATE_FORMAT(STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d'),
            '%Y-%m') m_rp,
    DATE_FORMAT(STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d'),
            '%Y') y_rp,
	rt.invoiceId,
	rt_dt.code,
    rt.customerCode,
    rt_dt.productcode,
    rt_dt.productname,
    rt.branchId,
    rt_dt.branchname,
    IF(rt_dt.quantity=0,0,rt_dt.quantity*-1) quantity,
    IF((rt_dt.quantity * rt_dt.price)=0,0,(rt_dt.quantity * rt_dt.price) * - 1) AS subtotal,
    IF((rt_dt.quantity * rt_dt.price)=0,0,(rt_dt.quantity * rt_dt.price) * - 1) AS subtotal_f,
    IFNULL(CASE
                WHEN
                    MID(rt_dt.soldByName,
                        REGEXP_INSTR(rt_dt.soldByName, 'NV[0-9]', 2),
                        6) = ''
                THEN
                    NULL
                ELSE MID(rt_dt.soldByName,
                    REGEXP_INSTR(rt_dt.soldByName, 'NV[0-9]', 2),
                    6)
            END,
            'Undefined') sales_code
FROM
    data_kiot_viet.returns_details_2022 rt_dt
        INNER JOIN
    data_kiot_viet.returns_2022 rt ON rt_dt.code = rt.code
WHERE
    rt.status = 1
        AND DATE(rt_dt.createdDate) BETWEEN '2023-01-01' AND '2023-01-31'
        AND invoiceId IS NULL
) rs_rt
GROUP BY createdDate,
m_rp,
y_rp,
branchID,
branchName,
invoiceId,
code,
customerCode,
sales_code
)
SELECT createdDate,
m_rp,
y_rp,
branchId,
branchName,
sales_code,
COUNT(DISTINCT code)*-1 Slg_HD,
COUNT(DISTINCT code)*-1 Slg_KH,
SUM(Slg_SKU)*-1 Slg_SKU,
SUM(quantity) quantity,
SUM(subtotal) subtotal,
SUM(subtotal_f) subtotal_f
FROM noinv_nocust_rt
WHERE customerCode IS NULL
GROUP BY createdDate,
m_rp,
y_rp,
branchId,
branchName,
sales_code;
COMMIT;

-- Return - Invoice ID Not Null - Customer code not null:
INSERT INTO data_kiot_viet.daily_sales_branch_prvm
WITH rt_rs AS (
SELECT createdDate,
m_rp,
y_rp,
branchID,
branchName,
invoiceId,
code,
customerCode,
sales_code,
COUNT(DISTINCT productcode) Slg_SKU,
SUM(quantity) quantity,
SUM(subtotal) subtotal,
SUM(subtotal_f) subtotal_f
FROM (
SELECT 
    STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d') createdDate,
    DATE_FORMAT(STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d'),
            '%Y-%m') m_rp,
    DATE_FORMAT(STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d'),
            '%Y') y_rp,
	rt.invoiceId,
	rt_dt.code,
    rt.customerCode,
    rt_dt.productcode,
    rt_dt.productname,
    rt.branchId,
    rt_dt.branchname,
    IF(rt_dt.quantity=0,0,rt_dt.quantity*-1) quantity,
    IF((rt_dt.quantity * rt_dt.price)=0,0,(rt_dt.quantity * rt_dt.price) * - 1) AS subtotal,
    IF((rt_dt.quantity * rt_dt.price)=0,0,(rt_dt.quantity * rt_dt.price) * - 1) AS subtotal_f,
    IFNULL(CASE
                WHEN
                    MID(rt_dt.soldByName,
                        REGEXP_INSTR(rt_dt.soldByName, 'NV[0-9]', 2),
                        6) = ''
                THEN
                    NULL
                ELSE MID(rt_dt.soldByName,
                    REGEXP_INSTR(rt_dt.soldByName, 'NV[0-9]', 2),
                    6)
            END,
            'Undefined') sales_code
FROM
    data_kiot_viet.returns_details_2022 rt_dt
        INNER JOIN
    data_kiot_viet.returns_2022 rt ON rt_dt.code = rt.code
WHERE
    rt.status = 1
        AND DATE(rt_dt.createdDate) BETWEEN '2023-01-01' AND '2023-01-31'
        AND invoiceId IS NOT NULL
        AND customerCode IS NOT NULL
) x
GROUP BY createdDate,
m_rp,
y_rp,
branchID,
branchName,
invoiceId,
code,
customerCode,
sales_code
),
inv_rs AS (
SELECT createdDate, 
m_rp, 
y_rp, 
branchId, 
branchName,
invoiceId,
code,
customercode,
sales_code,
COUNT(DISTINCT productcode) Slg_SKU,
SUM(quantity) quantity,
SUM(subtotal) subtotal,
SUM(subtotal_f) subtotal_f
FROM (
SELECT 
    STR_TO_DATE(inv_dt.createdDate, '%Y-%m-%d') createdDate,
    DATE_FORMAT(STR_TO_DATE(inv_dt.createdDate, '%Y-%m-%d'),'%Y-%m') m_rp,
    DATE_FORMAT(STR_TO_DATE(inv_dt.createdDate, '%Y-%m-%d'),'%Y') y_rp,
    inv_dt.code,
    inv.customerCode,
    inv_dt.invoiceid,
    inv_dt.productcode,
    inv_dt.productname,
    inv.branchId,
    inv_dt.branchname,
    inv_dt.quantity,
    inv_dt.subtotal,
    CASE WHEN (SUM(inv_dt.subtotal) OVER(PARTITION BY inv_dt.code) - inv_dt.total) > 0
		THEN  ROUND(inv_dt.subtotal - (SUM(inv_dt.subtotal) OVER(PARTITION BY inv_dt.code) - inv_dt.total)*inv_dt.subtotal/SUM(inv_dt.subtotal) OVER(PARTITION BY inv_dt.code),0)
		ELSE inv_dt.subtotal END subtotal_f,
    IFNULL(CASE
        WHEN
            MID(inv_dt.soldByName,
                REGEXP_INSTR(inv_dt.soldByName, 'NV[0-9]', 2),
                6) = ''
        THEN
            NULL
        ELSE MID(inv_dt.soldByName,
            REGEXP_INSTR(inv_dt.soldByName, 'NV[0-9]', 2),
            6)
    END,'Undefined') sales_code
FROM
    data_kiot_viet.invoice_details_2022 inv_dt INNER JOIN 
    data_kiot_viet.invoices_2022 inv
    ON  inv_dt.invoiceid = inv.id
WHERE EXISTS (SELECT NULL FROM rt_rs WHERE inv_dt.invoiceId = rt_rs.invoiceId)
AND inv.status <> 2
) rs
GROUP BY createdDate, 
m_rp, 
y_rp, 
branchId, 
branchName,
invoiceId,
code,
customercode,
sales_code
)
SELECT 
rt_rs.createdDate,
rt_rs.m_rp,
rt_rs.y_rp,
rt_rs.branchId,
rt_rs.branchName,
rt_rs.sales_code,
CASE WHEN rt_rs.Slg_SKU = inv_rs.Slg_SKU THEN COUNT(DISTINCT rt_rs.code)*-1 ELSE 0 END Slg_HD,
CASE WHEN rt_rs.Slg_SKU = inv_rs.Slg_SKU THEN COUNT(DISTINCT rt_rs.customercode)*-1 ELSE 0 END Slg_KH,
SUM(rt_rs.Slg_SKU)*-1 Slg_SKU,
SUM(rt_rs.quantity) quantity,
SUM(rt_rs.subtotal) subtotal,
SUM(rt_rs.subtotal_f) subtotal_f
FROM rt_rs
LEFT JOIN inv_rs
ON rt_rs.invoiceId = inv_rs.invoiceId
GROUP BY rt_rs.createdDate,
rt_rs.m_rp,
rt_rs.y_rp,
rt_rs.branchId,
rt_rs.branchName,
rt_rs.sales_code;
COMMIT;

-- Return - Invoice ID Not Null - Customer code null:
INSERT INTO data_kiot_viet.daily_sales_branch_prvm
WITH rt_rs AS (
SELECT createdDate,
m_rp,
y_rp,
branchID,
branchName,
invoiceId,
code,
customerCode,
sales_code,
COUNT(DISTINCT productcode) Slg_SKU,
SUM(quantity) quantity,
SUM(subtotal) subtotal,
SUM(subtotal_f) subtotal_f
FROM (
SELECT 
    STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d') createdDate,
    DATE_FORMAT(STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d'),
            '%Y-%m') m_rp,
    DATE_FORMAT(STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d'),
            '%Y') y_rp,
	rt.invoiceId,
	rt_dt.code,
    rt.customerCode,
    rt_dt.productcode,
    rt_dt.productname,
    rt.branchId,
    rt_dt.branchname,
    IF(rt_dt.quantity=0,0,rt_dt.quantity*-1) quantity,
    IF((rt_dt.quantity * rt_dt.price)=0,0,(rt_dt.quantity * rt_dt.price) * - 1) AS subtotal,
    IF((rt_dt.quantity * rt_dt.price)=0,0,(rt_dt.quantity * rt_dt.price) * - 1) AS subtotal_f,
    IFNULL(CASE
                WHEN
                    MID(rt_dt.soldByName,
                        REGEXP_INSTR(rt_dt.soldByName, 'NV[0-9]', 2),
                        6) = ''
                THEN
                    NULL
                ELSE MID(rt_dt.soldByName,
                    REGEXP_INSTR(rt_dt.soldByName, 'NV[0-9]', 2),
                    6)
            END,
            'Undefined') sales_code
FROM
    data_kiot_viet.returns_details_2022 rt_dt
        INNER JOIN
    data_kiot_viet.returns_2022 rt ON rt_dt.code = rt.code
WHERE
    rt.status = 1
        AND DATE(rt_dt.createdDate) BETWEEN '2023-01-01' AND '2023-01-31'
        AND invoiceId IS NOT NULL
        AND customerCode IS NULL
) x
GROUP BY createdDate,
m_rp,
y_rp,
branchID,
branchName,
invoiceId,
code,
customerCode,
sales_code
),
inv_rs AS (
SELECT createdDate, 
m_rp, 
y_rp, 
branchId, 
branchName,
invoiceId,
code,
customercode,
sales_code,
COUNT(DISTINCT productcode) Slg_SKU,
SUM(quantity) quantity,
SUM(subtotal) subtotal,
SUM(subtotal_f) subtotal_f
FROM (
SELECT 
    STR_TO_DATE(inv_dt.createdDate, '%Y-%m-%d') createdDate,
    DATE_FORMAT(STR_TO_DATE(inv_dt.createdDate, '%Y-%m-%d'),'%Y-%m') m_rp,
    DATE_FORMAT(STR_TO_DATE(inv_dt.createdDate, '%Y-%m-%d'),'%Y') y_rp,
    inv_dt.code,
    inv.customerCode,
    inv_dt.invoiceid,
    inv_dt.productcode,
    inv_dt.productname,
    inv.branchId,
    inv_dt.branchname,
    inv_dt.quantity,
    inv_dt.subtotal,
    CASE WHEN (SUM(inv_dt.subtotal) OVER(PARTITION BY inv_dt.code) - inv_dt.total) > 0
		THEN  ROUND(inv_dt.subtotal - (SUM(inv_dt.subtotal) OVER(PARTITION BY inv_dt.code) - inv_dt.total)*inv_dt.subtotal/SUM(inv_dt.subtotal) OVER(PARTITION BY inv_dt.code),0)
		ELSE inv_dt.subtotal END subtotal_f,
    IFNULL(CASE
        WHEN
            MID(inv_dt.soldByName,
                REGEXP_INSTR(inv_dt.soldByName, 'NV[0-9]', 2),
                6) = ''
        THEN
            NULL
        ELSE MID(inv_dt.soldByName,
            REGEXP_INSTR(inv_dt.soldByName, 'NV[0-9]', 2),
            6)
    END,'Undefined') sales_code
FROM
    data_kiot_viet.invoice_details_2022 inv_dt INNER JOIN 
    data_kiot_viet.invoices_2022 inv
    ON  inv_dt.invoiceid = inv.id
WHERE EXISTS (SELECT NULL FROM rt_rs WHERE inv_dt.invoiceId = rt_rs.invoiceId)
AND inv.status <> 2
) rs
GROUP BY createdDate, 
m_rp, 
y_rp, 
branchId, 
branchName,
invoiceId,
code,
customercode,
sales_code
)
SELECT 
rt_rs.createdDate,
rt_rs.m_rp,
rt_rs.y_rp,
rt_rs.branchId,
rt_rs.branchName,
rt_rs.sales_code,
CASE WHEN rt_rs.Slg_SKU = inv_rs.Slg_SKU THEN COUNT(DISTINCT rt_rs.code)*-1 ELSE 0 END Slg_HD,
CASE WHEN rt_rs.Slg_SKU = inv_rs.Slg_SKU THEN COUNT(DISTINCT rt_rs.code)*-1 ELSE 0 END Slg_KH,
SUM(rt_rs.Slg_SKU)*-1 Slg_SKU,
SUM(rt_rs.quantity) quantity,
SUM(rt_rs.subtotal) subtotal,
SUM(rt_rs.subtotal_f) subtotal_f
FROM rt_rs
LEFT JOIN inv_rs
ON rt_rs.invoiceId = inv_rs.invoiceId
GROUP BY rt_rs.createdDate,
rt_rs.m_rp,
rt_rs.y_rp,
rt_rs.branchId,
rt_rs.branchName,
rt_rs.sales_code;
COMMIT;

/*--------------------------------------------------!
03. Update lại mục tiêu
!--------------------------------------------------*/
SET SQL_SAFE_UPDATES = 0;

BEGIN

DECLARE ch_done1 INTEGER DEFAULT 0;
DECLARE v_date1 DATE DEFAULT NULL;
DECLARE v_branch1 VARCHAR(250) DEFAULT NULL;
DECLARE v_subtotal_d INTEGER DEFAULT NULL;
DECLARE v_week1 VARCHAR(250) DEFAULT NULL;


-- Cursor 1: Update mục tiêu ngày

DECLARE c_1 CURSOR FOR

SELECT 
 CONCAT('Tuần ',FLOOR((DayOfMonth(createdDate)-1)/7)+1) week_field, createdDate, branchname, SUM(subtotal_f) subtotal_f
FROM
    data_kiot_viet.daily_sales_branch_prvm
WHERE YEAR(createdDate) = YEAR(CURDATE())
AND MONTH(createdDate) = MONTH(CURDATE()) - 1
GROUP BY createdDate , branchname
ORDER BY branchname, createdDate;

DECLARE CONTINUE HANDLER FOR NOT FOUND SET ch_done1 = 1;

OPEN c_1;
c1loop: LOOP
FETCH NEXT FROM c_1 INTO v_week1, v_date1, v_branch1, v_subtotal_d;

IF ch_done1 = 1 
	THEN LEAVE c1loop;
END IF;

UPDATE data_kiot_viet.master_sales_target_prvm t1
LEFT JOIN (
SELECT * FROM (
SELECT date_field, week_field, cua_hang, MAX(date_field) OVER(PARTITION BY month_field, week_field) max_week_day,
muc_tieu_ngay
FROM data_kiot_viet.master_sales_target_prvm 
WHERE week_field = v_week1
AND cua_hang = v_branch1
ORDER BY cua_hang, date_field
) a
WHERE date_field = v_date1
) t2
ON t1.cua_hang = t2.cua_hang
AND t1.week_field = t2.week_field 
SET t1.muc_tieu_ngay = t1.muc_tieu_ngay + ROUND((t2.muc_tieu_ngay - v_subtotal_d)/DATEDIFF(t2.max_week_day,t2.date_field),0)
WHERE t1.date_field BETWEEN DATE_ADD(t2.date_field,INTERVAL 1 DAY) AND t2.max_week_day
AND t2.muc_tieu_ngay - v_subtotal_d > 0;

COMMIT;

END LOOP c1loop;

CLOSE c_1;
END;


TRUNCATE TABLE data_kiot_viet.master_sales_target_prvm_temp;

INSERT INTO data_kiot_viet.master_sales_target_prvm_temp
WITH emp_tv AS 
(
SELECT DISTINCT employee_code, employee_name, office_name, title, start_date, is_terminated, terminated_date  FROM data_hrm.employee_turnover_rp_v
WHERE is_terminated = 'Đang làm việc' OR (is_terminated = 'Nghỉ việc' AND DATE_FORMAT(terminated_date, '%Y-%m') = DATE_FORMAT(CURDATE(), '%Y-%m'))
)
SELECT rs.*, SUM(flag_nv) OVER(PARTITION BY date_field, cua_hang) slg_nv, ROUND(muc_tieu_ngay/SUM(flag_nv) OVER(PARTITION BY date_field, cua_hang),0) muc_tieu_nv,
ROUND(muc_tieu_ngay_bk/SUM(flag_nv) OVER(PARTITION BY date_field, cua_hang),0) muc_tieu_nv_bk
FROM (
SELECT td.*, emp_tv.*,
CASE WHEN td.date_field BETWEEN emp_tv.start_date AND IF(emp_tv.terminated_date = 0 OR emp_tv.terminated_date IS NULL,LAST_DAY(CURDATE()),emp_tv.terminated_date)
THEN 1 ELSE 0 END flag_nv
FROM data_kiot_viet.master_sales_target_prvm td
LEFT JOIN emp_tv
ON td.cua_hang = emp_tv.office_name
AND emp_tv.title = 'Nhân viên tư vấn'
WHERE td.cua_hang <> 'Hàng Ngoại Tỉnh (MKT)'
) rs;
COMMIT;

/*


COMMIT;
CREATE VIEW data_kiot_viet.daily_sales_asm_rp_prv_v AS
WITH rs_ds_br AS (
SELECT createdDate, 
m_rp, 
y_rp, 
branchid, 
branchName, 
sales_code, 
SUM(Slg_HD) Slg_HD,
SUM(Slg_KH) Slg_KH,
SUM(Slg_SKU) Slg_SKU, 
SUM(quantity) quantity,
SUM(subtotal) subtotal,
SUM(subtotal_f) subtotal_f
FROM 
(
SELECT * FROM data_kiot_viet.daily_sales_branch_all
WHERE YEAR(createdDate) = YEAR(CURDATE())
AND MONTH(createdDate) = MONTH(CURDATE()) - 1
) ds_br
GROUP BY createdDate, 
m_rp, 
y_rp, 
branchid, 
branchName, 
sales_code
),
emp_all AS
(
SELECT DISTINCT employee_code, employee_name, office_name, title, start_date, terminated_date, is_terminated FROM data_hrm.employee_turnover_rp_v
WHERE IF(terminated_date IS NULL OR terminated_date = 0,CURDATE(),terminated_date) BETWEEN DATE_ADD(CURDATE(),
        INTERVAL - DAY(CURDATE()) + 1 DAY) AND LAST_DAY(CURDATE())
)

SELECT tg.muc_tieu_nv,
tg.muc_tieu_nv_bk,
tg.slg_nv,
tg.flag_nv,
tg.date_field,
tg.month_field,
tg.num_day_of_month,
tg.week_field,
tg.num_day_of_week,
tg.cua_hang,
tg.ma_nv_asm,
emp_1.employee_name ten_nv_asm,
tg.muc_tieu_thang,
tg.muc_tieu_tuan,
tg.muc_tieu_ngay,
tg.muc_tieu_ngay_bk,
IFNULL(tg.aov,0) aov,
rs_ds_br.createdDate,
rs_ds_br.sales_code,
CONCAT(tg.employee_code,'-',tg.employee_name) ten_nv_tv,
tg.is_terminated,
tg.start_date,
tg.terminated_date,
IFNULL(rs_ds_br.Slg_HD,0) Slg_HD,
IFNULL(rs_ds_br.Slg_SKU,0) Slg_SKU,
IFNULL(rs_ds_br.quantity,0) quantity,
IFNULL(rs_ds_br.subtotal,0) subtotal,
IFNULL(rs_ds_br.subtotal_f,0) subtotal_f
FROM data_kiot_viet.master_sales_target_prvm_temp tg 
LEFT JOIN rs_ds_br
ON tg.date_field = rs_ds_br.createdDate
AND tg.cua_hang = rs_ds_br.branchName
AND tg.employee_code = rs_ds_br.sales_code
LEFT JOIN emp_all emp_1
ON tg.ma_nv_asm = emp_1.employee_code

UNION ALL

SELECT DISTINCT
0,
0,
0,
0,
tg.date_field,
tg.month_field,
tg.num_day_of_month,
tg.week_field,
tg.num_day_of_week,
tg.cua_hang,
tg.ma_nv_asm,
emp_1.employee_name ten_nv_asm,
tg.muc_tieu_thang,
tg.muc_tieu_tuan,
tg.muc_tieu_ngay,
tg.muc_tieu_ngay_bk,
IFNULL(tg.aov,0) aov,
rs_ds_br.createdDate,
rs_ds_br.sales_code,
'Undefined',
'Undefined',
'Undefined',
'Undefined',
IFNULL(rs_ds_br.Slg_HD,0) Slg_HD,
IFNULL(rs_ds_br.Slg_SKU,0) Slg_SKU,
IFNULL(rs_ds_br.quantity,0) quantity,
IFNULL(rs_ds_br.subtotal,0) subtotal,
IFNULL(rs_ds_br.subtotal_f,0) subtotal_f
FROM data_kiot_viet.master_sales_target_prvm_temp tg
LEFT JOIN rs_ds_br
ON tg.date_field = rs_ds_br.createdDate
AND tg.cua_hang = rs_ds_br.branchName
LEFT JOIN emp_all emp_1
ON tg.ma_nv_asm = emp_1.employee_code
WHERE (rs_ds_br.sales_code = 'Undefined'
OR NOT EXISTS (SELECT NULL FROM emp_all WHERE emp_all.employee_code = rs_ds_br.sales_code))

UNION ALL


SELECT DISTINCT
tg.muc_tieu_nv,
tg.muc_tieu_nv_bk,
tg.slg_nv,
tg.flag_nv,
tg.date_field,
tg.month_field,
tg.num_day_of_month,
tg.week_field,
tg.num_day_of_week,
tg.cua_hang,
tg.ma_nv_asm,
emp_1.employee_name ten_nv_asm,
tg.muc_tieu_thang,
tg.muc_tieu_tuan,
tg.muc_tieu_ngay,
tg.muc_tieu_ngay_bk,
IFNULL(tg.aov,0) aov,
rs_ds_br.createdDate,
rs_ds_br.sales_code,
CONCAT(emp_2.employee_code,'-',emp_2.employee_name) ten_nv_tv,
emp_2.is_terminated,
emp_2.start_date,
emp_2.terminated_date,
IFNULL(rs_ds_br.Slg_HD,0) Slg_HD,
IFNULL(rs_ds_br.Slg_SKU,0) Slg_SKU,
IFNULL(rs_ds_br.quantity,0) quantity,
IFNULL(rs_ds_br.subtotal,0) subtotal,
IFNULL(rs_ds_br.subtotal_f,0) subtotal_f
FROM data_kiot_viet.master_sales_target_prvm_temp tg
LEFT JOIN rs_ds_br
ON tg.date_field = rs_ds_br.createdDate
AND tg.cua_hang = rs_ds_br.branchName
INNER JOIN emp_all emp_1
ON tg.ma_nv_asm = emp_1.employee_code
INNER JOIN emp_all emp_2
ON rs_ds_br.sales_code = emp_2.employee_code
WHERE EXISTS (SELECT NULL FROM emp_all WHERE emp_all.employee_code = rs_ds_br.sales_code AND emp_all.title <> 'Nhân viên tư vấn')

UNION ALL


SELECT DISTINCT
0,
0,
0,
0,
tg.date_field,
tg.month_field,
tg.num_day_of_month,
tg.week_field,
tg.num_day_of_week,
rs_ds_br1.branchName,
rs_ds_br1.ma_nv_asm,
emp_1.employee_name ten_nv_asm,
rs_ds_br1.muc_tieu_thang,
rs_ds_br1.muc_tieu_tuan,
rs_ds_br1.muc_tieu_ngay,
0,
IFNULL(rs_ds_br1.aov,0) aov,
rs_ds_br1.createdDate,
rs_ds_br1.sales_code,
CONCAT(emp_2.employee_code,'-',emp_2.employee_name) ten_nv_tv,
emp_2.is_terminated,
emp_2.start_date,
emp_2.terminated_date,
IFNULL(rs_ds_br1.Slg_HD,0) Slg_HD,
IFNULL(rs_ds_br1.Slg_SKU,0) Slg_SKU,
IFNULL(rs_ds_br1.quantity,0) quantity,
IFNULL(rs_ds_br1.subtotal,0) subtotal,
IFNULL(rs_ds_br1.subtotal_f,0) subtotal_f
FROM data_kiot_viet.master_sales_target_prvm_temp tg
INNER JOIN (
select DISTINCT rs_ds_br.*, tar.muc_tieu_thang,  tar.muc_tieu_tuan, tar.muc_tieu_ngay, tar.ma_nv_asm, tar.aov from rs_ds_br
INNER JOIN data_kiot_viet.master_sales_target_daily_temp tar ON rs_ds_br.branchname = tar.cua_hang AND rs_ds_br.createdDate = tar.date_field
) rs_ds_br1
ON tg.date_field = rs_ds_br1.createdDate
AND tg.cua_hang <> rs_ds_br1.branchName
AND tg.employee_code = rs_ds_br1.sales_code
INNER JOIN emp_all emp_1
ON rs_ds_br1.ma_nv_asm = emp_1.employee_code
INNER JOIN emp_all emp_2
ON rs_ds_br1.sales_code = emp_2.employee_code
ORDER BY cua_hang asc, date_field asc, muc_tieu_thang DESC;
COMMIT;
*/

END