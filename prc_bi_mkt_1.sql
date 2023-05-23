CREATE DEFINER=`root`@`%` PROCEDURE `prc_bi_mkt_1`()
BEGIN
SET sql_mode = 'NO_ENGINE_SUBSTITUTION';

/*--------------------------------------------------!
01. Số bán 30 ngày
!--------------------------------------------------*/

TRUNCATE TABLE data_kiot_viet.Supply_30days;
INSERT INTO data_kiot_viet.Supply_30days
WITH prd As (
SELECT 
    SYSDATE() event_date,
    prd.categoryName,
    ivt.productId,
    ivt.productCode,
    prd.fullname productname,
    SUM(ivt.onHand) onHand
FROM
    data_kiot_viet.inventory_daily ivt
        LEFT JOIN
    data_kiot_viet.product prd ON ivt.productId = prd.id
WHERE
    DATE_FORMAT(ivt.reportingdate, '%Y-%m-%d') = CURDATE()
        AND ivt.branchId NOT IN (
        '253446', 
        '151636',
        '116012',
        '253441',
        '253443',
        '297997',
        '253383',
        '298145',
        '253384',
        '253442',
        '253387',
        '253410',
        '253386',
        '253385',
        '152715',
        '253444')
GROUP BY SYSDATE() , prd.categoryName , ivt.productId , ivt.productCode , prd.fullname
),
inv As (
SELECT 
    invd.productId,
    inv.customercode,
    SUM(invd.subTotal) Total,
    SUM(invd.quantity) quantity
FROM
    data_kiot_viet.invoices inv,
    data_kiot_viet.invoice_details invd
WHERE
    inv.code = invd.code 
    AND inv.status <> '2' ##Trạng thái khác Đã huỷ
    AND DATE(inv.purchaseDate) BETWEEN DATE_ADD(DATE_ADD(CURDATE(), INTERVAL - 1 DAY),
        INTERVAL - DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY)) + 1 DAY) AND LAST_DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY))
        ##Hoá đơn phát sinh trong 30 ngày gần nhất
GROUP BY invd.productId, inv.customercode
),
rtn As (
SELECT 
    rtd.productId,
    rt.customerCode,
    SUM(rtd.quantity) return_quantity,
    SUM(rtd.quantity * rtd.price) return_amount
FROM
    data_kiot_viet.returns rt
        LEFT JOIN
    data_kiot_viet.returns_details rtd ON rt.code = rtd.code
WHERE
    rt.status = '1' ##Trạng thái phiếu trả = Đã trả
        AND DATE(rt.returnDate) BETWEEN DATE_ADD(DATE_ADD(CURDATE(), INTERVAL - 1 DAY),
        INTERVAL - DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY)) + 1 DAY) AND LAST_DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY))
        ##Ngày trả hàng trong 30 ngày gần nhất
        GROUP BY rt.branchId, rt.customerCode
)
Select prd.event_date,
prd.categoryName,  prd.productId, prd.productCode, prd.productname, inv.customercode, 
CAST(prd.onhand AS DECIMAL) onhand,
IFNULL(inv.quantity,0) Quantity,
IFNULL(inv.Total,0) Total,
IFNULL(rtn.return_quantity,0) Return_Quantity,
IFNULL(rtn.return_amount,0) Return_Amount,
IFNULL(inv.quantity,0) - IFNULL(rtn.return_quantity,0) Quantity_f,
IFNULL(inv.Total,0) - IFNULL(rtn.return_amount,0) Total_f
FROM prd
LEFT JOIN inv
ON prd.productId = inv.productId
LEFT JOIN rtn
ON prd.productId = rtn.productId
AND inv.customercode = rtn.customercode
WHERE prd.categoryname NOT IN (N'Tài sản công ty (TSCT)',N'Thiết bị IT',N'Văn phòng phẩm',N'Vật tư','POSM','Error','Voucher',N'Thiết bị Setup - Bảo trì')
;
COMMIT;

/*--------------------------------------------------!
02. Báo cáo số bán 30 ngày
!--------------------------------------------------*/

TRUNCATE TABLE data_kiot_viet.rp_30days_mkt;
INSERT INTO data_kiot_viet.rp_30days_mkt
SELECT a.categoryname, a.productcode, a.productname, a.customercode,oh_tk.cost, a.quantity_f,a.total_f, oh_tk.onhand_tk  FROM data_kiot_viet.Supply_30days a
LEFT JOIN (
SELECT SUM(onhand) onhand_tk, SUM(cost) cost, productcode
FROM
    data_kiot_viet.inventory_daily
WHERE
    DATE_FORMAT(reportingdate, '%Y-%m-%d') = CURDATE()
        AND branchid = '253685' -- Tổng kho
GROUP BY productcode
) oh_tk
ON a.productcode = oh_tk.productcode
WHERE a.quantity_f <> 0;
COMMIT;

/*--------------------------------------------------!
03. Báo cáo Daily Sales Marketing
!--------------------------------------------------*/

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
SELECT 
    createdDate,
    productcode,
    SUM(quantity) quantity,
    SUM(subtotal) subtotal
FROM
    data_kiot_viet.daily_sales
WHERE
    DATE_FORMAT(createdDate, '%Y-%m') = DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL - 1 DAY),
            '%Y-%m')
GROUP BY createdDate , productcode
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

/*--------------------------------------------------!
4. Phân loại khách hàng lẻ/cũ/mới theo khu vực
!--------------------------------------------------*/

TRUNCATE TABLE data_kiot_viet.customer_by_region;

INSERT INTO data_kiot_viet.customer_by_region
WITH inv_rs AS (
SELECT 
    inv_2022.code,
    inv_2022.purchaseDate,
    CASE WHEN inv_2022.branchname LIKE '%HP%' THEN 'Hải Phòng'
		WHEN inv_2022.branchname LIKE '%HD%' THEN 'Hải Dương'
        WHEN inv_2022.branchname LIKE '%QN%' THEN 'Quảng Ninh' ELSE NULL END Region,
    CASE WHEN inv_2022.customerCode = '' THEN inv_2022.code ELSE inv_2022.customerCode END customerCode,
    CASE WHEN inv_2022.customerCode = '' THEN 'Khách lẻ' ELSE NULL END flag_cust_1,
    ROW_NUMBER() OVER(PARTITION BY customerCode, MONTH(purchaseDate) ORDER BY purchaseDate) rn
FROM
    data_kiot_viet.invoices_2022 inv_2022
WHERE
    DATE(inv_2022.purchaseDate) BETWEEN DATE_ADD(DATE_ADD(CURDATE(), INTERVAL - 1 DAY),
        INTERVAL - DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY)) + 1 DAY) AND LAST_DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY))
        AND inv_2022.status <> 2
AND inv_2022.branchname <> 'Hàng Ngoại Tỉnh (MKT)'
),
slg_hd AS
(
SELECT 
    createdDate,
    CASE
        WHEN branchName LIKE '%HP%' THEN 'Hải Phòng'
        WHEN branchName LIKE '%HD%' THEN 'Hải Dương'
        WHEN branchName LIKE '%QN%' THEN 'Quảng Ninh'
        ELSE NULL
    END Region,
    SUM(slg_HD) slg_HD
FROM
    data_kiot_viet.daily_sales_branch
WHERE
    branchName <> 'Hàng Ngoại Tỉnh (MKT)'
GROUP BY createdDate , CASE
    WHEN branchName LIKE '%HP%' THEN 'Hải Phòng'
    WHEN branchName LIKE '%HD%' THEN 'Hải Dương'
    WHEN branchName LIKE '%QN%' THEN 'Quảng Ninh'
    ELSE NULL
END
)
SELECT DISTINCT DATE(purchaseDate) purchaseDate,
rs.region,
customerCode,
customer_name,
DATE(createdDate_cust) createdDate_cust,
flag_cust_2,
slg_hd.Slg_HD
FROM (
SELECT inv_rs.*,
cust.name customer_name,
cust.createdDate createdDate_cust,
cust.contactNumber,
CASE WHEN inv_rs.flag_cust_1 IS NULL THEN 
	CASE WHEN cust.createdDate NOT BETWEEN DATE_ADD(DATE_ADD(CURDATE(), INTERVAL - 1 DAY),
        INTERVAL - DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY)) + 1 DAY) AND LAST_DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY))
        THEN '3.Khách hàng cũ'
	WHEN cust.createdDate BETWEEN DATE_ADD(DATE_ADD(CURDATE(), INTERVAL - 1 DAY),
        INTERVAL - DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY)) + 1 DAY) AND LAST_DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY))
        AND rn = 1 THEN '2.Khách hàng mới'
	WHEN cust.createdDate BETWEEN DATE_ADD(DATE_ADD(CURDATE(), INTERVAL - 1 DAY),
        INTERVAL - DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY)) + 1 DAY) AND LAST_DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY))
        AND rn <> 1 THEN '3.Khách hàng cũ'
        ELSE NULL END ELSE '1.Khách hàng lẻ' END flag_cust_2
FROM inv_rs
LEFT JOIN data_kiot_viet.customer cust
ON inv_rs.customerCode = cust.code
)
rs
LEFT JOIN slg_hd
ON rs.purchaseDate = slg_hd.createdDate
AND rs.region = slg_hd.region;
COMMIT;

END