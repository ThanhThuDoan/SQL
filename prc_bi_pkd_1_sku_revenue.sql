CREATE DEFINER=`root`@`%` PROCEDURE `prc_bi_pkd_1_sku_revenue`()
BEGIN

SET SQL_MODE = 'NO_ENGINE_SUBSTITUTION';
SET SQL_SAFE_UPDATES = 0;

/*--------------------------------------------------!
Doanh số theo ngành hàng - Tháng hiện tại
!--------------------------------------------------*/

TRUNCATE TABLE data_kiot_viet.daily_sales_product_kd;

INSERT INTO data_kiot_viet.daily_sales_product_kd
SELECT ds.createdDate, ds.m_rp, ds.productcode,
ds.productname,
prd.categoryName,
CASE WHEN prd.categoryname_1 IS NULL THEN
		CASE WHEN (prd.categoryName LIKE 'Q-%' OR prd.categoryName = 'Quà khuyến mại (Q)') THEN 'Quà khuyến mại (Q)'
        WHEN prd.categoryname LIKE 'Đ.QATT%' THEN '7.Thời trang'
        WHEN (prd.categoryname LIKE 'DDCB%' OR  prd.categoryname LIKE 'DDGD%') THEN '3.Đồ dùng'
        WHEN prd.categoryname LIKE 'DAU%' THEN '1.Đồ ăn uống'
		WHEN prd.categoryName  =  '2.Đồ chơi (DC)' THEN '2.Đồ chơi (DC)'
		WHEN (prd.categoryName LIKE 'SUA%' OR prd.categoryName LIKE 'Sữa%') THEN '6.Sữa'
        WHEN prd.categoryName LIKE 'HMP%' THEN '5.Hóa mỹ phẩm'
        WHEN prd.categoryName LIKE 'BIM%' THEN '0.Bỉm'
        ELSE NULL END
	ELSE prd.categoryname_1 END categoryName_1,
ds.branchName,
SUM(ds.quantity) quantity,
SUM(ds.subtotal) subtotal
FROM data_kiot_viet.daily_sales ds
LEFT JOIN data_kiot_viet.product_categories prd
ON ds.productcode = prd.code
WHERE YEAR(ds.createdDate) = YEAR(DATE_ADD(CURDATE(), INTERVAL -1 DAY))
AND MONTH(ds.createdDate)  = MONTH(DATE_ADD(CURDATE(), INTERVAL -1 DAY))
AND prd.categoryName NOT IN ('Tài sản công ty (TSCT)' , 'POSM',
        'Error',
        'Vật tư',
        'Voucher',
        'Văn phòng phẩm',
        'Thiết bị IT',
        'Thiết bị Setup - Bảo trì')
GROUP BY 
ds.createdDate, ds.m_rp, ds.productcode,
ds.productname,
prd.categoryName,
CASE WHEN prd.categoryname_1 IS NULL THEN
		CASE WHEN (prd.categoryName LIKE 'Q-%' OR prd.categoryName = 'Quà khuyến mại (Q)') THEN 'Quà khuyến mại (Q)'
        WHEN prd.categoryname LIKE 'Đ.QATT%' THEN '7.Thời trang'
        WHEN (prd.categoryname LIKE 'DDCB%' OR  prd.categoryname LIKE 'DDGD%') THEN '3.Đồ dùng'
        WHEN prd.categoryname LIKE 'DAU%' THEN '1.Đồ ăn uống'
		WHEN prd.categoryName  =  '2.Đồ chơi (DC)' THEN '2.Đồ chơi (DC)'
		WHEN (prd.categoryName LIKE 'SUA%' OR prd.categoryName LIKE 'Sữa%') THEN '6.Sữa'
        WHEN prd.categoryName LIKE 'HMP%' THEN '5.Hóa mỹ phẩm'
        WHEN prd.categoryName LIKE 'BIM%' THEN '0.Bỉm'
        ELSE NULL END
	ELSE prd.categoryname_1 END,
ds.branchName

UNION ALL

SELECT ds.m_rp, ds.m_rp, ds.productcode,
ds.productname,
prd.categoryName,
CASE WHEN prd.categoryname_1 IS NULL THEN
		CASE WHEN (prd.categoryName LIKE 'Q-%' OR prd.categoryName = 'Quà khuyến mại (Q)') THEN 'Quà khuyến mại (Q)'
        WHEN prd.categoryname LIKE 'Đ.QATT%' THEN '7.Thời trang'
        WHEN (prd.categoryname LIKE 'DDCB%' OR  prd.categoryname LIKE 'DDGD%') THEN '3.Đồ dùng'
        WHEN prd.categoryname LIKE 'DAU%' THEN '1.Đồ ăn uống'
		WHEN prd.categoryName  =  '2.Đồ chơi (DC)' THEN '2.Đồ chơi (DC)'
		WHEN (prd.categoryName LIKE 'SUA%' OR prd.categoryName LIKE 'Sữa%') THEN '6.Sữa'
        WHEN prd.categoryName LIKE 'HMP%' THEN '5.Hóa mỹ phẩm'
        WHEN prd.categoryName LIKE 'BIM%' THEN '0.Bỉm'
        ELSE NULL END
	ELSE prd.categoryname_1 END categoryName_1,
ds.branchName,
SUM(ds.quantity) quantity,
SUM(ds.subtotal) subtotal
FROM data_kiot_viet.daily_sales ds
LEFT JOIN data_kiot_viet.product_categories prd
ON ds.productcode = prd.code
WHERE YEAR(ds.createdDate) = YEAR(DATE_ADD(CURDATE(), INTERVAL -1 DAY))
AND MONTH(ds.createdDate)  = MONTH(DATE_ADD(CURDATE(), INTERVAL -1 DAY)) - 1
AND prd.categoryName NOT IN ('Tài sản công ty (TSCT)' , 'POSM',
        'Error',
        'Vật tư',
        'Voucher',
        'Văn phòng phẩm',
        'Thiết bị IT',
        'Thiết bị Setup - Bảo trì')
GROUP BY 
 ds.m_rp, ds.m_rp, ds.productcode,
ds.productname,
prd.categoryName,
CASE WHEN prd.categoryname_1 IS NULL THEN
		CASE WHEN (prd.categoryName LIKE 'Q-%' OR prd.categoryName = 'Quà khuyến mại (Q)') THEN 'Quà khuyến mại (Q)'
        WHEN prd.categoryname LIKE 'Đ.QATT%' THEN '7.Thời trang'
        WHEN (prd.categoryname LIKE 'DDCB%' OR  prd.categoryname LIKE 'DDGD%') THEN '3.Đồ dùng'
        WHEN prd.categoryname LIKE 'DAU%' THEN '1.Đồ ăn uống'
		WHEN prd.categoryName  =  '2.Đồ chơi (DC)' THEN '2.Đồ chơi (DC)'
		WHEN (prd.categoryName LIKE 'SUA%' OR prd.categoryName LIKE 'Sữa%') THEN '6.Sữa'
        WHEN prd.categoryName LIKE 'HMP%' THEN '5.Hóa mỹ phẩm'
        WHEN prd.categoryName LIKE 'BIM%' THEN '0.Bỉm'
        ELSE NULL END
	ELSE prd.categoryname_1 END,
ds.branchName

UNION ALL

SELECT ds.m_rp, ds.m_rp, ds.productcode,
ds.productname,
prd.categoryName,
CASE WHEN prd.categoryname_1 IS NULL THEN
		CASE WHEN (prd.categoryName LIKE 'Q-%' OR prd.categoryName = 'Quà khuyến mại (Q)') THEN 'Quà khuyến mại (Q)'
        WHEN prd.categoryname LIKE 'Đ.QATT%' THEN '7.Thời trang'
        WHEN (prd.categoryname LIKE 'DDCB%' OR  prd.categoryname LIKE 'DDGD%') THEN '3.Đồ dùng'
        WHEN prd.categoryname LIKE 'DAU%' THEN '1.Đồ ăn uống'
		WHEN prd.categoryName  =  '2.Đồ chơi (DC)' THEN '2.Đồ chơi (DC)'
		WHEN (prd.categoryName LIKE 'SUA%' OR prd.categoryName LIKE 'Sữa%') THEN '6.Sữa'
        WHEN prd.categoryName LIKE 'HMP%' THEN '5.Hóa mỹ phẩm'
        WHEN prd.categoryName LIKE 'BIM%' THEN '0.Bỉm'
        ELSE NULL END
	ELSE prd.categoryname_1 END categoryName_1,
ds.branchName,
SUM(ds.quantity) quantity,
SUM(ds.subtotal) subtotal
FROM data_kiot_viet.daily_sales ds
LEFT JOIN data_kiot_viet.product_categories prd
ON ds.productcode = prd.code
WHERE YEAR(ds.createdDate) = YEAR(DATE_ADD(CURDATE(), INTERVAL -1 DAY))
AND MONTH(ds.createdDate)  = MONTH(DATE_ADD(CURDATE(), INTERVAL -1 DAY)) - 2
AND prd.categoryName NOT IN ('Tài sản công ty (TSCT)' , 'POSM',
        'Error',
        'Vật tư',
        'Voucher',
        'Văn phòng phẩm',
        'Thiết bị IT',
        'Thiết bị Setup - Bảo trì')
GROUP BY 
 ds.m_rp, ds.m_rp, ds.productcode,
ds.productname,
prd.categoryName,
CASE WHEN prd.categoryname_1 IS NULL THEN
		CASE WHEN (prd.categoryName LIKE 'Q-%' OR prd.categoryName = 'Quà khuyến mại (Q)') THEN 'Quà khuyến mại (Q)'
        WHEN prd.categoryname LIKE 'Đ.QATT%' THEN '7.Thời trang'
        WHEN (prd.categoryname LIKE 'DDCB%' OR  prd.categoryname LIKE 'DDGD%') THEN '3.Đồ dùng'
        WHEN prd.categoryname LIKE 'DAU%' THEN '1.Đồ ăn uống'
		WHEN prd.categoryName  =  '2.Đồ chơi (DC)' THEN '2.Đồ chơi (DC)'
		WHEN (prd.categoryName LIKE 'SUA%' OR prd.categoryName LIKE 'Sữa%') THEN '6.Sữa'
        WHEN prd.categoryName LIKE 'HMP%' THEN '5.Hóa mỹ phẩm'
        WHEN prd.categoryName LIKE 'BIM%' THEN '0.Bỉm'
        ELSE NULL END
	ELSE prd.categoryname_1 END,
ds.branchName;

COMMIT;

/*--------------------------------------------------!
Doanh số theo ngành hàng - 2 tháng gần nhất
!--------------------------------------------------*/

TRUNCATE TABLE data_kiot_viet.pkd_sosanh_cungky_nh;

INSERT INTO data_kiot_viet.pkd_sosanh_cungky_nh
SELECT ds.createdDate,ds.m_rp, ds.productcode,
ds.productname,
prd.categoryName,
CASE WHEN prd.categoryname_1 IS NULL THEN
		CASE WHEN (prd.categoryName LIKE 'Q-%' OR prd.categoryName = 'Quà khuyến mại (Q)') THEN 'Quà khuyến mại (Q)'
        WHEN prd.categoryname LIKE 'Đ.QATT%' THEN '7.Thời trang'
        WHEN (prd.categoryname LIKE 'DDCB%' OR  prd.categoryname LIKE 'DDGD%') THEN '3.Đồ dùng'
        WHEN prd.categoryname LIKE 'DAU%' THEN '1.Đồ ăn uống'
		WHEN prd.categoryName  =  '2.Đồ chơi (DC)' THEN '2.Đồ chơi (DC)'
		WHEN (prd.categoryName LIKE 'SUA%' OR prd.categoryName LIKE 'Sữa%') THEN '6.Sữa'
        WHEN prd.categoryName LIKE 'HMP%' THEN '5.Hóa mỹ phẩm'
        WHEN prd.categoryName LIKE 'BIM%' THEN '0.Bỉm'
        ELSE NULL END
	ELSE prd.categoryname_1 END categoryName_1,
ds.branchName,
SUM(ds.quantity) quantity,
SUM(ds.subtotal) subtotal
FROM data_kiot_viet.daily_sales ds
LEFT JOIN data_kiot_viet.product_categories prd
ON ds.productcode = prd.code
WHERE MONTH(ds.createdDate) IN (MONTH(NOW() - INTERVAL 2 MONTH),MONTH(NOW() - INTERVAL 1 MONTH))
    AND (
          YEAR(ds.createdDate) = YEAR(NOW()) 
        OR 
          YEAR(ds.createdDate) = YEAR(NOW() - INTERVAL 2 MONTH))
AND prd.categoryName NOT IN ('Tài sản công ty (TSCT)' , 'POSM',
        'Error',
        'Vật tư',
        'Voucher',
        'Văn phòng phẩm',
        'Thiết bị IT',
        'Thiết bị Setup - Bảo trì')
GROUP BY
ds.createdDate, ds.m_rp, ds.productcode,
ds.productname,
prd.categoryName,
CASE WHEN prd.categoryname_1 IS NULL THEN
		CASE WHEN (prd.categoryName LIKE 'Q-%' OR prd.categoryName = 'Quà khuyến mại (Q)') THEN 'Quà khuyến mại (Q)'
        WHEN prd.categoryname LIKE 'Đ.QATT%' THEN '7.Thời trang'
        WHEN (prd.categoryname LIKE 'DDCB%' OR  prd.categoryname LIKE 'DDGD%') THEN '3.Đồ dùng'
        WHEN prd.categoryname LIKE 'DAU%' THEN '1.Đồ ăn uống'
		WHEN prd.categoryName  =  '2.Đồ chơi (DC)' THEN '2.Đồ chơi (DC)'
		WHEN (prd.categoryName LIKE 'SUA%' OR prd.categoryName LIKE 'Sữa%') THEN '6.Sữa'
        WHEN prd.categoryName LIKE 'HMP%' THEN '5.Hóa mỹ phẩm'
        WHEN prd.categoryName LIKE 'BIM%' THEN '0.Bỉm'
        ELSE NULL END
	ELSE prd.categoryname_1 END,
ds.branchName;

COMMIT;

/*--------------------------------------------------!
Báo cáo so sánh cùng kỳ doanh số ngành hàng MTD
!--------------------------------------------------*/

/*
CREATE VIEW data_kiot_viet.pkd_sosanh_cungky_nh_mtd AS
SELECT
DATE_FORMAT(createdDate,'%Y-%m') month_field,
productcode,
productName,
categoryName,
categoryName_1,
branchName,
SUM(quantity) quantity,
SUM(subtotal) subtotal
FROM data_kiot_viet.daily_sales_product_kd 
WHERE MONTH(createdDate) = MONTH(DATE_ADD(CURDATE(), INTERVAL -1 DAY))
AND createdDate <= DATE_ADD(CURDATE(), INTERVAL -1 DAY)
GROUP BY 
DATE_FORMAT(createdDate,'%Y-%m'),
productcode,
productName,
categoryName,
categoryName_1,
branchName

UNION ALL

SELECT m_rp,
productcode,
productName,
categoryName,
categoryName_1,
branchName,
SUM(quantity) quantity,
SUM(subtotal) subtotal
FROM data_kiot_viet.pkd_sosanh_cungky_nh a
WHERE MONTH(createdDate) = MONTH(DATE_ADD(CURDATE(), INTERVAL -1 MONTH))
AND createdDate < DATE_ADD(CURDATE(), INTERVAL -1 MONTH)
GROUP BY 
m_rp,
productcode,
productName,
categoryName,
categoryName_1,
branchName;
COMMIT;
*/

/*Báo cáo hoá đơn Bỉm & Sữa*/

TRUNCATE TABLE data_kiot_viet.pkd_hoadon_bimsua;
INSERT INTO data_kiot_viet.pkd_hoadon_bimsua
WITH aa AS (
SELECT DATE(inv_dt.createdDate) createdDate,
inv_dt.code, 
inv_dt.branchName,inv_dt.invoiceId, inv_dt.productcode, inv_dt.productName, inv_dt.quantity,
COUNT(inv_dt.productcode) OVER(PARTITION BY inv_dt.code) slg_sku,
SUM(inv_dt.quantity) OVER(PARTITION BY inv_dt.code) sum_quantity,
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
    END,'Undefined') sales_code,
    IFNULL(emp.name,'Undefined') sales_name
FROM data_kiot_viet.invoice_details_2022 inv_dt
INNER JOIN data_kiot_viet.invoices_2022 inv
ON inv_dt.code = inv.code
LEFT JOIN data_hrm.employees emp
ON IFNULL(CASE
        WHEN
            MID(inv_dt.soldByName,
                REGEXP_INSTR(inv_dt.soldByName, 'NV[0-9]', 2),
                6) = ''
        THEN
            NULL
        ELSE MID(inv_dt.soldByName,
            REGEXP_INSTR(inv_dt.soldByName, 'NV[0-9]', 2),
            6)
    END,'Undefined') = emp.code
LEFT JOIN data_kiot_viet.product prd
ON inv_dt.productcode = prd.code
WHERE inv.status <> 2
AND EXISTS (
SELECT NULL FROM data_kiot_viet.product prd WHERE prd.code = inv_dt.productcode
AND (categoryName LIKE 'Bỉm%' OR categoryName LIKE 'Sữa%')
AND MONTH(inv_dt.createdDate) = MONTH(DATE_ADD(CURDATE(), INTERVAL -1 DAY))
AND YEAR(inv_dt.createdDate) = YEAR(DATE_ADD(CURDATE(), INTERVAL -1 DAY)))
),
slg_hd AS
(
SELECT DATE(createdDate) createdDate,
DATE_FORMAT(createdDate,'%Y-%m') m_rp,
branchName, COUNT(DISTINCT code) slg_hd 
FROM data_kiot_viet.invoices_2022 inv WHERE MONTH(createdDate) = MONTH(DATE_ADD(CURDATE(), INTERVAL -1 DAY))
AND YEAR(createdDate) = YEAR(DATE_ADD(CURDATE(), INTERVAL -1 DAY))
GROUP BY DATE(createdDate),DATE_FORMAT(createdDate,'%Y-%m'), branchName 
),
fn AS (
SELECT 
slg_hd.createdDate,
slg_hd.m_rp,
slg_hd.branchName,
aa.code, aa.invoiceId, aa.productCode,
aa.productName, aa.quantity,
slg_hd.slg_hd AS `Số lượng hoá đơn`,
aa.sales_code, aa.sales_name
FROM slg_hd
LEFT JOIN aa
ON aa.branchName = slg_hd.branchName
AND aa.createdDate = slg_hd.createdDate

AND EXISTS (
SELECT NULL FROM
(
SELECT DISTINCT code, COUNT(productcode) OVER(PARTITION BY code) slg_sku
FROM data_kiot_viet.invoice_details_2022 
WHERE MONTH(createdDate) = MONTH(DATE_ADD(CURDATE(), INTERVAL -1 DAY))
AND YEAR(createdDate) = YEAR(DATE_ADD(CURDATE(), INTERVAL -1 DAY))
) a
WHERE aa.code = a.code
AND a.slg_sku = aa.slg_sku
)
WHERE aa.code IS NOT NULL)
SELECT fn.*, emp_2.name asm FROM fn
LEFT JOIN data_kiot_viet.master_sales_target tg
ON fn.m_rp = CONVERt(tg.thang,CHAR)
AND REPLACE(fn.branchName," ","") = REPLACE(tg.cua_hang," ","")
LEFT JOIN data_hrm.employees emp_2
ON tg.ma_nv_asm = emp_2.code
ORDER BY createdDate, branchName, code
;
COMMIT;
END