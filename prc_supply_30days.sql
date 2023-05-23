CREATE DEFINER=`root`@`%` PROCEDURE `prc_supply_30days`(
)
BEGIN

/*--------------------------------------------------!
01. SỐ BÁN 90 DAYS
!--------------------------------------------------*/

TRUNCATE TABLE data_kiot_viet.Supply_90days;
SET sql_mode = 'NO_ENGINE_SUBSTITUTION';
INSERT INTO data_kiot_viet.Supply_90days
WITH prd AS (
SELECT sysdate() event_date,
	ivt.reportingDate,
    prd.categoryName,
    ivt.productId,
    ivt.productCode,
    prd.fullname productname,
    ivt.branchName,
    ivt.onHand,
    cost_tk.cost,
    prd.basePrice
FROM
    (SELECT * FROM data_kiot_viet.inventory_daily WHERE DATE(reportingdate)= CURDATE()) ivt
		LEFT JOIN 
	(SELECT productcode, cost FROM data_kiot_viet.inventory_daily WHERE DATE(reportingdate) = CURDATE() AND branchname = N'01.TỔNG KHO 2') cost_tk
		ON ivt.productcode = cost_tk.productcode
        LEFT JOIN
    data_kiot_viet.product prd ON ivt.productId = prd.id
    WHERE ivt.branchid NOT IN ('253446','151636','116012','253441','253443','297997','253383','298145','253384','253442','253387','253410','253386','253385','152715','253444')
    AND ivt.CATEGORYNAME NOT IN (N'Tài sản công ty (TSCT)',N'Thiết bị IT',N'Văn phòng phẩm',
    #N'Vật tư',
    'POSM','Error','Voucher',N'Thiết bị Setup - Bảo trì',N'Quà khuyến mại (Q)')
),
ds AS (
SELECT branchname, productcode, SUM(quantity) quantity, SUM(subtotal) subtotal FROM
data_kiot_viet.daily_sales WHERE createdDate BETWEEN DATE_ADD(CURDATE(),
        INTERVAL - 91 DAY) AND DATE_ADD(CURDATE(),
        INTERVAL - 1 DAY)
        GROUP BY branchname, productcode
)
SELECT
prd.event_date, STR_TO_DATE(prd.reportingDate,'%Y-%m-%d') reportingDate, 
prd.branchName,
prd.categoryName,
prd.productId,
prd.productCode,
prd.productname,
prd.cost,
CAST(prd.onhand AS DECIMAL) onhand,
IFNULL(ds.quantity,0) Quantity_f,
IFNULL(ds.subtotal,0) Total_f,
ROUND(prd.cost*IFNULL(ds.quantity,0),0) Cost_Total,
IFNULL(ds.subtotal,0) - ROUND(prd.cost*IFNULL(ds.quantity,0),0) Profit,
ROUND(CAST(prd.onhand AS DECIMAL)*prd.cost,0) Onhand_Amount
FROM prd
LEFT JOIN ds
ON  prd.branchname = ds.branchname
AND prd.productcode = ds.productcode
;
COMMIT;

/*--------------------------------------------------!
02. SỐ BÁN 30 DAYS
!--------------------------------------------------*/

TRUNCATE TABLE data_kiot_viet.Supply_30days;
SET sql_mode = 'NO_ENGINE_SUBSTITUTION';
INSERT INTO data_kiot_viet.Supply_30days
With prd As (
SELECT sysdate() event_date,
    prd.categoryName,
    ivt.productId,
    ivt.productCode,
    prd.fullname productname,
    SUM(ivt.onHand) onHand
FROM
    data_kiot_viet.inventory_daily ivt
        LEFT JOIN
    data_kiot_viet.product prd ON ivt.productId = prd.id
    WHERE  Date_format(ivt.reportingdate, '%Y-%m-%d') = CURDATE()
    AND ivt.branchId NOT IN ('253446','151636','116012','253441','253443','297997','253383','298145','253384','253442','253387','253410','253386','253385','152715','253444')
    GROUP BY sysdate(),
    prd.categoryName,
    ivt.productId,
    ivt.productCode,
    prd.fullname
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
        aND DATE(rt.returnDate) BETWEEN DATE_ADD(DATE_ADD(CURDATE(), INTERVAL - 1 DAY),
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
From prd
Left Join inv
On prd.productId = inv.productId
Left Join rtn
On prd.productId = rtn.productId
AND inv.customercode = rtn.customercode
WHERE prd.CATEGORYNAME NOT IN (N'Tài sản công ty (TSCT)',N'Thiết bị IT',N'Văn phòng phẩm',N'Vật tư','POSM','Error','Voucher',N'Thiết bị Setup - Bảo trì')
;
COMMIT;

/*--------------------------------------------------!
03. BẢNG KẾT QUẢ
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
04. Báo cáo biến động tồn kho
!--------------------------------------------------*/

TRUNCATE TABLE data_kiot_viet.inventory_30days;

INSERT INTO data_kiot_viet.inventory_30days
SELECT DISTINCT  DATE_ADD(DATE(reportingdate),INTERVAL -1 DAY) reportingdate,
    CASE WHEN DAY(reportingdate) = 1 THEN  DAY(LAST_DAY(reportingdate - INTERVAL 1 MONTH)) ELSE DAY(reportingdate) - 1 END d_rp,
	CASE WHEN DAY(reportingdate) = 1 THEN  MONTH(LAST_DAY(reportingdate - INTERVAL 1 MONTH)) ELSE MONTH(reportingdate) END m_rp,
    CASE WHEN DAY(reportingdate) = 1 THEN  YEAR(LAST_DAY(reportingdate - INTERVAL 1 MONTH)) ELSE YEAR(reportingdate) END  y_rp,
    productcode,
    productname,
    branchname,
    CAST(onhand AS DECIMAL) onhand,
	CAST(onhand AS DECIMAL) * cost onhand_amount
FROM data_kiot_viet.inventory invt
WHERE DATE(invt.reportingDate) BETWEEN DATE_ADD(CURDATE(), INTERVAL - 91 DAY)
         AND DATE_ADD(CURDATE(), INTERVAL -1 DAY)
         and invt.branchId NOT IN ('253446','151636','116012','253441','253443','297997','253383','298145','253384','253442','253387','253410','253386','253385','152715','253444')
    AND NOT EXISTS (SELECT NULL FROM data_kiot_viet.product prd WHERE prd.categoryName IN (N'Tài sản công ty (TSCT)',N'Thiết bị IT',N'Văn phòng phẩm',N'Vật tư','POSM','Error','Voucher',N'Thiết bị Setup - Bảo trì',N'Quà khuyến mại (Q)')
    AND prd.id = invt.branchId);
    
COMMIT;
END