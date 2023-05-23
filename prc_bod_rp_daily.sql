CREATE DEFINER=`root`@`%` PROCEDURE `prc_bod_rp_daily`()
BEGIN
DELETE FROM data_kiot_viet.bod_report_6th_profit
WHERE DATE(createdDate) BETWEEN DATE_ADD(CURDATE(),INTERVAL -31 DAY) AND DATE_ADD(CURDATE(),INTERVAL -1 DAY);

COMMIT;

INSERT INTO data_kiot_viet.bod_report_6th_profit
WITH b AS 
(
SELECT DATE(inv_dt.createdDate) createdDate,
inv_dt.productCode, inv_dt.branchName, COUNT(DISTINCT inv.customercode) slg_kh
FROM data_kiot_viet.invoice_details_2022 inv_dt
INNER JOIN data_kiot_viet.invoices_2022 inv
ON inv_dt.code = inv.code
WHERE DATE(inv_dt.createdDate) BETWEEN DATE_ADD(CURDATE(),INTERVAL -31 DAY) AND DATE_ADD(CURDATE(),INTERVAL -1 DAY)
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
WHERE ds.createdDate BETWEEN DATE_ADD(CURDATE(),INTERVAL -31 DAY) AND DATE_ADD(CURDATE(),INTERVAL -1 DAY);

COMMIT;
END