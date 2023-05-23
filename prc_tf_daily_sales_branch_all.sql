CREATE DEFINER=`root`@`%` PROCEDURE `prc_tf_daily_sales_branch_all`(
IN fromdate DATE,
IN todate DATE
)
BEGIN

SET SQL_MODE = 'NO_ENGINE_SUBSTITUTION';
SET SQL_SAFE_UPDATES = 0;

-- Xoá dữ liệu trong 31 ngày:

DELETE FROM data_kiot_viet.daily_sales_branch_all
WHERE createdDate BETWEEN fromdate AND todate;

COMMIT;

/*--------------------------------------------------!
01. Get data doanh thu & return theo cửa hàng
!--------------------------------------------------*/

TRUNCATE TABLE data_kiot_viet.daily_sales_branch_temp;

INSERT INTO data_kiot_viet.daily_sales_branch_temp
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
WHERE DATE(inv_dt.createdDate) BETWEEN fromdate AND todate
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
INSERT INTO data_kiot_viet.daily_sales_branch_temp
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
WHERE DATE(inv_dt.createdDate) BETWEEN fromdate AND todate
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
INSERT INTO data_kiot_viet.daily_sales_branch_temp
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
        AND DATE(rt_dt.createdDate) BETWEEN fromdate AND todate
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
INSERT INTO data_kiot_viet.daily_sales_branch_temp
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
        AND DATE(rt_dt.createdDate) BETWEEN fromdate AND todate
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
INSERT INTO data_kiot_viet.daily_sales_branch_temp
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
        AND DATE(rt_dt.createdDate) BETWEEN fromdate AND todate
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
INSERT INTO data_kiot_viet.daily_sales_branch_temp
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
        AND DATE(rt_dt.createdDate) BETWEEN fromdate AND todate
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
Bảng kết quả
!--------------------------------------------------*/

#TRUNCATE TABLE data_kiot_viet.daily_sales_branch_all;

INSERT INTO data_kiot_viet.daily_sales_branch_all
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
data_kiot_viet.daily_sales_branch_temp ds_br
WHERE DATE(createdDate) BETWEEN fromdate AND todate
GROUP BY createdDate, 
m_rp, 
y_rp, 
branchid, 
branchName, 
sales_code;
COMMIT;

END