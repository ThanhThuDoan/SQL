CREATE DEFINER=`root`@`%` PROCEDURE `prc_bod_report`()
BEGIN

/*--------------------------------------------------!
01. Báo cáo tồn kho, số bán, số lượng khách hàng 30 ngày
!--------------------------------------------------*/

-- Tồn kho ngày hôm qua, số bán 30 ngày

TRUNCATE TABLE data_kiot_viet.bod_revenue_onhand_30days;
INSERT INTO data_kiot_viet.bod_revenue_onhand_30days
WITH invt AS
(
SELECT SUM(onhand) onhand, productcode, productname FROM data_kiot_viet.inventory 
WHERE DATE(reportingdate) = CURDATE()
GROUP BY productcode, productname
),
 invt_cost AS 
(
SELECT cost, productcode FROM data_kiot_viet.inventory 
WHERE DATE(reportingdate) = CURDATE()
AND branchname = '01.TỔNG KHO 2'
)
SELECT dmhh.ten_function,
invt.productcode,
invt.productname,
prd.basePrice,
invt_cost.cost,
invt.onhand,
ds.quantity,
ds.subtotal
FROM invt
LEFT JOIN 
(
SELECT SUM(quantity) quantity, SUM(subtotal) subtotal, productcode, productname FROM data_kiot_viet.daily_sales
WHERE createdDate BETWEEN DATE_ADD(CURDATE(), INTERVAL -31 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) GROUP BY productcode, productname
) ds 
ON invt.productcode = ds.productcode
LEFT JOIN data_kiot_viet.master_dmhh dmhh 
ON invt.productcode = dmhh.ma_hang
LEFT JOIN invt_cost
ON invt.productcode = invt_cost.productcode
LEFT JOIN data_kiot_viet.product prd
ON invt.productcode = prd.code;

COMMIT;

-- Số lượng khách hàng 30 ngày

TRUNCATE TABLE data_kiot_viet.bod_revenue_onhand_30days_cust;
INSERT INTO data_kiot_viet.bod_revenue_onhand_30days_cust
WITH aa AS (
SELECT
CASE WHEN inv.customerCode = '' THEN inv.code ELSE inv.customerCode END customerCode ,
inv_dt.productCode,
inv_dt.branchName,
SUM(inv_dt.subtotal) subtotal FROM data_kiot_viet.invoices_2022 inv
INNER JOIN data_kiot_viet.invoice_details_2022 inv_dt
ON inv.code = inv_dt.code
WHERE inv.status <> 2
AND DATE(inv_dt.createdDATE) BETWEEN DATE_ADD(CURDATE(), INTERVAL -31 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY)
GROUP BY CASE WHEN inv.customerCode = '' THEN inv.code ELSE inv.customerCode END,
inv_dt.productCode,
inv_dt.branchName
)
SELECT  productCode "Mã sản phẩm" ,COUNT(DISTINCT customercode) "Số lượng khách hàng", SUM(subtotal) "Doanh thu" FROM aa
GROUP BY productCode;

COMMIT;

/*--------------------------------------------------!
02. Báo cáo tồn kho, số bán, số lượng khách hàng 1 ngày
!--------------------------------------------------*/

TRUNCATE TABLE data_kiot_viet.bod_revenue_onhand_1days_cust;

INSERT INTO data_kiot_viet.bod_revenue_onhand_1days_cust
WITH invt AS
(
SELECT SUM(onhand) onhand, productcode, productname FROM data_kiot_viet.inventory 
WHERE DATE(reportingdate) = CURDATE()
GROUP BY productcode, productname
),
 invt_cost AS 
(
SELECT cost, productcode FROM data_kiot_viet.inventory 
WHERE DATE(reportingdate) = CURDATE()
AND branchname = '01.TỔNG KHO 2'
),
slg_cust AS
(
SELECT
CASE WHEN inv.customerCode = '' THEN inv.code ELSE inv.customerCode END customerCode ,
inv_dt.productCode,
inv_dt.branchName,
SUM(inv_dt.subtotal) subtotal FROM data_kiot_viet.invoices_2022 inv
INNER JOIN data_kiot_viet.invoice_details_2022 inv_dt
ON inv.code = inv_dt.code
WHERE inv.status <> 2
AND DATE(inv_dt.createdDATE) = DATE_ADD(CURDATE(), INTERVAL -1 DAY)
GROUP BY CASE WHEN inv.customerCode = '' THEN inv.code ELSE inv.customerCode END,
inv_dt.productCode,
inv_dt.branchName
)
SELECT dmhh.ten_function "Tên function",
invt.productcode "Mã sản phẩm",
invt.productname "Tên sản phẩm",
prd.basePrice "Giá bán",
invt_cost.cost "Giá vốn",
invt.onhand "Số lượng tồn kho",
ds.quantity "Số bán",
ds.subtotal "Doanh thu",
IFNULL(cust.`Số lượng khách hàng`,0) "Số lượng khách hàng"
FROM invt
LEFT JOIN 
(
SELECT SUM(quantity) quantity, SUM(subtotal) subtotal, productcode, productname FROM data_kiot_viet.daily_sales
WHERE createdDate BETWEEN DATE_ADD(CURDATE(), INTERVAL -31 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) GROUP BY productcode, productname
) ds
ON invt.productcode = ds.productcode
LEFT JOIN
(
SELECT  productCode "Mã sản phẩm" ,COUNT(DISTINCT customercode) "Số lượng khách hàng" FROM slg_cust
GROUP BY productCode
) cust
ON cust.`Mã sản phẩm` = invt.productcode
LEFT JOIN data_kiot_viet.master_dmhh dmhh 
ON invt.productcode = dmhh.ma_hang
LEFT JOIN invt_cost
ON invt.productcode = invt_cost.productcode
LEFT JOIN data_kiot_viet.product prd
ON invt.productcode = prd.code;

COMMIT;

/*Báo cáo Doanh số ngành hàng 6th*/

TRUNCATE TABLE data_kiot_viet.bod_report_6th_profit;

INSERT INTO data_kiot_viet.bod_report_6th_profit

WITH b AS 
(
SELECT DATE(inv_dt.createdDate) createdDate,
inv_dt.productCode, inv_dt.branchName, COUNT(DISTINCT inv.customercode) slg_kh
FROM data_kiot_viet.invoice_details_2022 inv_dt
INNER JOIN data_kiot_viet.invoices_2022 inv
ON inv_dt.code = inv.code
WHERE DATE(inv_dt.createdDate) BETWEEN data_kiot_viet.first_day(date_add(curdate(), interval - 6 month))
AND last_day(date_add(curdate(), interval - 1 month))
AND NOT EXISTS
(
SELECT rt_dt.*, rt.invoiceID FROM data_kiot_viet.returns_2022 rt
INNER JOIN data_kiot_viet.returns_details_2022 rt_dt
ON rt.code = rt_dt.code
WHERE inv_dt.invoiceId = rt.invoiceId
AND inv_dt.productcode = rt_dt.productcode
AND inv_dt.quantity = rt_dt.quantity
)
GROUP BY DATE(inv_dt.createdDate),
inv_dt.productCode, inv_dt.branchName
)
SELECT DISTINCT
ds.createdDate, ds.m_rp, prd.categoryName, prd.categoryName_1,
ds.productcode, ds.productname, ds.branchname, 
ds.quantity, ds.subtotal,ds.profit, b.slg_kh
FROM 
(SELECT createdDate, m_rp, productcode, productname, branchname,
SUM(quantity) quantity, SUM(subtotal) subtotal, SUM(profit) profit FROM data_kiot_viet.daily_sales
GROUP BY createdDate, m_rp, productcode, productname, branchname) ds
LEFT JOIN b 
ON ds.createdDate = DATE(b.createdDate) AND ds.productcode = b.productcode AND ds.branchname = b.branchName
#LEFT JOIN data_kiot_viet.invoice_details_2022 c
#ON ds.createdDate = DATE(c.createdDate) AND ds.productcode = c.productcode AND ds.branchname = c.branchName
#LEFT JOIN data_kiot_viet.inventory_daily id
#ON ds.productcode = id.productcode
#AND ds.branchName = id.branchName
#LEFT JOIN data_kiot_viet.master_dmhh dmhh
#ON ds.productcode = dmhh.ma_hang
LEFT JOIN data_kiot_viet.product_categories prd
ON ds.productcode = prd.code
WHERE ds.createdDate BETWEEN data_kiot_viet.first_day(date_add(curdate(), interval - 6 month))
AND last_day(date_add(curdate(), interval - 1 month));
COMMIT;

/*Báo cáo tỷ suất lợi nhuận*/

TRUNCATE TABLE data_kiot_viet.bod_report_tsln_v;

INSERT INTO data_kiot_viet.bod_report_tsln_v
WITH aa AS (
SELECT 
    ds.createdDate,
    ds.m_rp,
    ds.y_rp,
    CASE WHEN prd.categoryName IN ('10.Hàng ký gửi','10. Hàng ký gửi') THEN '10.Hàng ký gửi' 
    WHEN prd.categoryName_1 IS NULL AND prd.categoryName LIKE '%Bỉm%' THEN '0.Bỉm' 
    WHEN prd.categoryName_1 IS NULL AND prd.categoryName LIKE 'DAU%' THEN '1.Đồ ăn uống'
    WHEN prd.categoryName_1 IS NULL AND prd.categoryName LIKE 'HMP%' THEN '5.Hóa mỹ phẩm'
    WHEN prd.categoryName_1 IS NULL AND prd.categoryName LIKE '%Sữa%' THEN '6.Sữa'
    ELSE prd.categoryName_1 END categoryName_1,
    prd.categoryName,
    ds.productcode,
    ds.productName,
    ds.branchName,
    ds.quantity,
    ds.subtotal,
    ds.sales_code,
    ds.name sales_name,
    ds.cost,
    ds.profit,
    SUM(ds.subtotal) OVER(PARTITION BY ds.branchName) sum_subtotal,
    SUM(ds.profit) OVER(PARTITION BY ds.branchName) sum_profit
FROM
    data_kiot_viet.daily_sales ds
        LEFT JOIN
    data_kiot_viet.product_categories prd ON ds.productcode = prd.code
WHERE
    ds.createdDate BETWEEN DATE_ADD(DATE_ADD(CURDATE(), INTERVAL - 1 DAY),
        INTERVAL - DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY)) + 1 DAY) AND LAST_DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY))
)
SELECT aa.*,
DENSE_RANK() OVER(ORDER BY aa.sum_subtotal DESC) rank_subtotal,
DENSE_RANK() OVER(ORDER BY aa.sum_profit DESC) rank_profit
FROM aa;

COMMIT;

END