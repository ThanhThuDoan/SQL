CREATE DEFINER=`root`@`%` PROCEDURE `prc_bi_pkd_1_customer`()
BEGIN

/*--------------------------------------------------!
Phân loại khách hàng lẻ/cũ/mới theo từng cửa hàng - tháng hiện tại
!--------------------------------------------------*/

TRUNCATE TABLE data_kiot_viet.customer_by_branch;

INSERT INTO data_kiot_viet.customer_by_branch
WITH inv_rs AS (
SELECT 
    inv_2022.code,
    inv_2022.purchaseDate,
    inv_2022.branchname,
    CASE WHEN inv_2022.customerCode = '' THEN inv_2022.code ELSE inv_2022.customerCode END customerCode,
    CASE WHEN inv_2022.customerCode = '' THEN 'Khách lẻ' ELSE NULL END flag_cust_1,
    ROW_NUMBER() OVER(PARTITION BY customerCode, MONTH(purchaseDate) ORDER BY purchaseDate) rn
FROM
    data_kiot_viet.invoices_2022 inv_2022
WHERE
    DATE(inv_2022.purchaseDate) BETWEEN DATE_ADD(DATE_ADD(CURDATE(), INTERVAL - 1 DAY),
        INTERVAL - DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY)) + 1 DAY) AND LAST_DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY))
        AND inv_2022.status <> 2
)
SELECT DISTINCT DATE(purchaseDate) purchaseDate,
branchname,
ten_nv_asm,
customerCode,
customer_name,
DATE(createdDate_cust) createdDate_cust,
flag_cust_2
FROM (
SELECT inv_rs.*,
sales_asm.ten_nv_asm,
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
LEFT JOIN 
(
select tar.*, emp.name ten_nv_asm from data_kiot_viet.master_sales_target tar
LEFT JOIN data_hrm.employees emp
ON tar.ma_nv_asm = emp.code
WHERE tar.thang = DATE_FORMAT(CURDATE(),'%Y-%m')
) sales_asm
ON inv_rs.branchname = sales_asm.cua_hang
)
rs
WHERE rs.branchname <> 'Hàng Ngoại Tỉnh (MKT)';
COMMIT;

/*--------------------------------------------------!
Phân loại khách hàng lẻ/cũ/mới theo từng cửa hàng - 6 tháng
!--------------------------------------------------*/

END