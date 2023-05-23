CREATE DEFINER=`root`@`%` PROCEDURE `prc_customer`()
BEGIN

/*--------------------------------------------------!
0. Clean dữ liệu khách hàng
!--------------------------------------------------*/
SET sql_mode = 'NO_ENGINE_SUBSTITUTION';
TRUNCATE TABLE data_kiot_viet.customer_clean_data;

INSERT INTO data_kiot_viet.customer_clean_data
SELECT 
    cust.code customer_code,
    PROPER(cust.name) customer_name,
    DATE(cust.birthDate) birthDate,
    CASE
        WHEN DATE(cust.birthDate) IS NULL THEN NULL
        ELSE ROUND(DATEDIFF(CURDATE(), DATE(cust.birthDate)) / 30,
                0)
    END num_month_age,
    CASE
        WHEN cust.contactNumber = '' THEN NULL
        ELSE cust.contactNumber
    END contactNumber,
    br.branchName,
    CASE
        WHEN IFNULL(PROPER(cust.address), '') = '' THEN NULL
        ELSE PROPER(cust.address)
    END address,
    CASE
        WHEN IFNULL(cust.wardName, '') = '' THEN NULL
        ELSE cust.wardName
    END wardName,
    CASE
        WHEN IFNULL(cust.locationName, '') = '' THEN NULL
        ELSE MID(cust.locationName,
            REGEXP_INSTR(cust.locationName, '-') + 2,
            LENGTH(cust.locationName) - REGEXP_INSTR(cust.locationName, '-') - 1)
    END districtName,
    CASE
        WHEN IFNULL(cust.locationName, '') = '' THEN NULL
        ELSE LEFT(cust.locationName,
            REGEXP_INSTR(cust.locationName, '-') - 2)
    END cityName,
    CASE
        WHEN IFNULL(cust.locationName, '') = '' THEN NULL
        WHEN
            (REGEXP_INSTR(cust.locationName, '-') <> 0
                AND cust.locationName <> ''
                AND IFNULL(cust.wardName, '') <> '')
        THEN
            CONCAT(cust.wardName,
                    ' - ',
                    MID(cust.locationName,
                        REGEXP_INSTR(cust.locationName, '-') + 2,
                        LENGTH(cust.locationName) - REGEXP_INSTR(cust.locationName, '-') - 1),
                    ' - ',
                    LEFT(cust.locationName,
                        REGEXP_INSTR(cust.locationName, '-') - 2))
        WHEN
            (REGEXP_INSTR(cust.locationName, '-') <> 0
                AND cust.locationName <> ''
                AND IFNULL(cust.wardName, '') = '')
        THEN
            CONCAT(MID(cust.locationName,
                        REGEXP_INSTR(cust.locationName, '-') + 2,
                        LENGTH(cust.locationName) - REGEXP_INSTR(cust.locationName, '-') - 1),
                    ' - ',
                    LEFT(cust.locationName,
                        REGEXP_INSTR(cust.locationName, '-') - 2))
        ELSE NULL
    END locationName,
    cust.totalInvoiced,
    cust.totalRevenue,
    cust.totalPoint,
    cust.gender,
    CASE
        WHEN IFNULL(cust.email, '') = '' THEN NULL
        ELSE cust.email
    END email,
    DATE(cust.modifiedDate) modifiedDate,
    DATE(cust.createdDate) createdDate
FROM
    data_kiot_viet.customer cust
        LEFT JOIN
    data_kiot_viet.branch br ON cust.branchId = br.Id;

COMMIT;

/*--------------------------------------------------!
1. Phân loại khách hàng lẻ/cũ/mới theo từng cửa hàng
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
2. Phân loại khách hàng lẻ/cũ/mới theo khu vực
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
SELECT createdDate, CASE WHEN branchName LIKE '%HP%' THEN 'Hải Phòng'
WHEN branchName LIKE '%HD%' THEN 'Hải Dương'
WHEN branchName LIKE '%QN%' THEN 'Quảng Ninh'
ELSE NULL END Region,
SUM(slg_HD) slg_HD
FROM data_kiot_viet.daily_sales_branch
WHERE branchName <> 'Hàng Ngoại Tỉnh (MKT)'
GROUP BY createdDate,
CASE WHEN branchName LIKE '%HP%' THEN 'Hải Phòng'
WHEN branchName LIKE '%HD%' THEN 'Hải Dương'
WHEN branchName LIKE '%QN%' THEN 'Quảng Ninh'
ELSE NULL END
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

/*--------------------------------------------------!
3. Dữ liệu web nội bộ
!--------------------------------------------------*/

-- Dữ liệu tra cứu sales: 

TRUNCATE TABLE data_kiot_viet.tra_cuu_sales_incentive;
INSERT INTO data_kiot_viet.tra_cuu_sales_incentive

WITH aa AS (
SELECT createdDate,
    sales_code, name, title,branchname, b.categoryName_1 nhom_hang, SUM(subtotal) subtotal
FROM
    data_kiot_viet.daily_sales a
    LEFT JOIN data_kiot_viet.product_categories b
    ON a.productcode = b.code
WHERE
    createdDate BETWEEN DATE_ADD(DATE_ADD(CURDATE(), INTERVAL - 1 DAY),
        INTERVAL - DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY)) + 1 DAY) AND LAST_DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY))
        AND b.categoryName_1 NOT IN ('Tài sản công ty (TSCT)','Văn phòng phẩm','Vật tư','Quà khuyến mại (Q)')
        AND a.title <> 'QLCH'
        GROUP BY createdDate,sales_code, name, title, b.categoryName_1,branchname
        )
        SELECT
        aa.createdDate,
		aa.sales_code, 
        aa.name, 
        aa.title, 
        aa.branchname,
        'Thưởng nhóm ngành hàng' incentive_category,
        aa.nhom_hang,
        aa.subtotal,
        bb.incentive_rate,
        ROUND(bb.incentive_rate*aa.subtotal,0) incentive_amount  FROM aa
        LEFT JOIN data_kiot_viet.master_incentive_category bb
        ON aa.nhom_hang = bb.categoryname
	
    UNION ALL
    
    SELECT
ds.createdDate,
ds.sales_code, 
ds.name, 
ds.title,
ds.branchname,
'Thưởng định hướng' incentive_category,
ds.productname,
ds.quantity,
ict_dh.thuong incentive_rate,
ds.quantity*ict_dh.thuong incentive_amount
FROM
    data_kiot_viet.daily_sales ds
    INNER JOIN 
    (
    SELECT
STR_TO_DATE(tu_ngay, '%e/%m/%Y') tu_ngay,
STR_TO_DATE(den_ngay, '%e/%m/%Y') den_ngay,
ma_hang,
thuong
FROM data_kiot_viet.master_incentive_dinhhuong
    ) ict_dh
ON ict_dh.ma_hang = ds.productcode
AND ds.createdDate BETWEEN ict_dh.tu_ngay AND ict_dh.den_ngay
WHERE
    ds.createdDate BETWEEN DATE_ADD(DATE_ADD(CURDATE(), INTERVAL - 1 DAY),
        INTERVAL - DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY)) + 1 DAY) AND LAST_DAY(DATE_ADD(CURDATE(), INTERVAL - 1 DAY))
AND ds.subtotal <> 0
AND ds.title <> 'QLCH'
ORDER BY createdDate, sales_code, branchname, nhom_hang;

COMMIT;

-- Dữ liệu web nội bộ:

TRUNCATE TABLE report.total;
INSERT INTO report.total
SELECT createdDate, sales_code, SUM(slg_hd) slg_hd, SUM(slg_SKU) slg_SKU, SUM(subtotal_f) subtotal_f  
FROM data_kiot_viet.daily_sales_branch
GROUP BY createdDate, sales_code;
COMMIT;

TRUNCATE TABLE report.sales;
INSERT INTO report.sales
SELECT createdDate, sales_code, name, title, branchName, nhom_hang, subtotal, incentive_rate, incentive_amount FROM tra_cuu_sales_v;
COMMIT;

END