CREATE DEFINER=`root`@`%` PROCEDURE `prc_tf_daily_sales_product_all`(
IN fromdate DATE,
IN todate DATE
)
BEGIN

DECLARE rp_date DATE;
DECLARE ch_done INTEGER DEFAULT 0;

SET SQL_SAFE_UPDATES = 0;

TRUNCATE TABLE data_kiot_viet.daily_sales_temp;

-- Xoá ngày dữ liệu gần nhất

DELETE FROM data_kiot_viet.daily_sales WHERE CreatedDate BETWEEN fromdate AND todate;

COMMIT;

/*--------------------------------------------------!
Lấy danh sách các ngày dữ liệu chưa có trong bảng kết quả
!--------------------------------------------------*/

SET sql_mode = 'NO_ENGINE_SUBSTITUTION';

TRUNCATE TABLE data_kiot_viet.data_time;
INSERT INTO data_kiot_viet.data_time

SELECT DISTINCT
    DATE(createdDate) dayid
FROM
    data_kiot_viet.invoices_2022 inv
WHERE
    NOT EXISTS( SELECT 
            NULL
        FROM
            data_kiot_viet.daily_sales ds
        WHERE
            ds.createdDate = DATE(inv.createdDate));
COMMIT;

/*--------------------------------------------------!
Declare Cursor + Loop
!--------------------------------------------------*/

c1loop: LOOP
BEGIN
DECLARE c_d CURSOR FOR 
SELECT MIN(dayid) FROM data_kiot_viet.data_time;

DECLARE CONTINUE HANDLER FOR NOT FOUND SET ch_done = 1;

IF ch_done = 1 OR (SELECT count(*) FROM data_kiot_viet.data_time) = 0
	THEN LEAVE c1loop;
END IF;

SET sql_mode = 'NO_ENGINE_SUBSTITUTION';

OPEN c_d;
FETCH FROM c_d INTO rp_date;

/*--------------------------------------------------!
01. Lấy danh sách Invoice theo ngày
!--------------------------------------------------*/

INSERT INTO data_kiot_viet.daily_sales_temp

SELECT createdDate, m_rp, y_rp, productcode, productname,branchname, SUM(quantity) quantity, SUM(subtotal_f) subtotal, sales_code 
FROM (
SELECT DISTINCT
    STR_TO_DATE(inv_dt.createdDate, '%Y-%m-%d') createdDate,
    DATE_FORMAT(STR_TO_DATE(inv_dt.createdDate, '%Y-%m-%d'),'%Y-%m') m_rp,
    DATE_FORMAT(STR_TO_DATE(inv_dt.createdDate, '%Y-%m-%d'),'%Y') y_rp,
    inv_dt.code,
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
WHERE DATE(inv_dt.createdDate) = DATE(rp_date)
AND inv.status <> 2
) rs_inv
GROUP BY createdDate, m_rp, y_rp, productcode, productname,branchname, sales_code


UNION ALL
/*--------------------------------------------------!
02. Lấy danh sách return theo ngày
!--------------------------------------------------*/

-- TH1: KHÔNG CÓ INVOICE ID

SELECT createdDate,m_rp,y_rp,productcode,productname,branchname,SUM(quantity) quantity, SUM(subtotal) subtotal,sales_code
FROM 
(
SELECT DISTINCT
    STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d') createdDate,
    DATE_FORMAT(STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d'),
            '%Y-%m') m_rp,
    DATE_FORMAT(STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d'),
            '%Y') y_rp,
    rt_dt.productcode,
    rt_dt.productname,
    rt_dt.branchname,
    IF(rt_dt.quantity=0,0,rt_dt.quantity*-1) quantity,
    IF((rt_dt.quantity * rt_dt.price)=0,0,(rt_dt.quantity * rt_dt.price) * - 1) AS subtotal,
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
        AND STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d') = DATE(rp_date)
        AND invoiceId IS NULL
) rs_rt_noinv
GROUP BY createdDate,m_rp,y_rp,productcode,productname,branchname,sales_code

-- TH2: CÓ INVOICE ID

UNION ALL

SELECT createdDate, m_rp, y_rp, productcode, productname,branchname, SUM(quantity) quantity, SUM(subtotal) subtotal, sales_code FROM
(
SELECT DISTINCT rt_rs.createdDate,
rt_rs.m_rp,
rt_rs.y_rp,
rt_rs.code,
rt_rs.invoiceId,
rt_rs.productcode,
rt_rs.productname,
rt_rs.branchid,
rt_rs.branchname,
IF(rt_rs.quantity=0,0,rt_rs.quantity*-1) quantity,
IF(inv_rs.subtotal_f=0,0,inv_rs.subtotal_f*-1)*rt_rs.quantity/inv_rs.quantity AS subtotal,
rt_rs.sales_code
FROM 
(
SELECT DISTINCT
    STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d') createdDate,
    DATE_FORMAT(STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d'),
            '%Y-%m') m_rp,
    DATE_FORMAT(STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d'),
            '%Y') y_rp,
    rt_dt.code,
    rt.invoiceId,
    rt_dt.productcode,
    rt_dt.productname,
    rt.branchid,
    rt_dt.branchname,
    rt_dt.quantity,
    rt_dt.quantity * rt_dt.price AS subtotal,
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
WHERE rt.status = 1 AND STR_TO_DATE(rt_dt.createdDate, '%Y-%m-%d') = DATE(rp_date)
        AND rt.invoiceId IS NOT NULL
) rt_rs
INNER JOIN 
(
SELECT DISTINCT
    inv_dt.invoiceid,
    inv_dt.productcode,
    inv_dt.quantity,
    CASE WHEN (SUM(inv_dt.subtotal) OVER(PARTITION BY inv_dt.code) - inv_dt.total) > 0
		THEN  ROUND(inv_dt.subtotal - (SUM(inv_dt.subtotal) OVER(PARTITION BY inv_dt.code) - inv_dt.total)*inv_dt.subtotal/SUM(inv_dt.subtotal) OVER(PARTITION BY inv_dt.code),0)
		ELSE inv_dt.subtotal END subtotal_f
FROM
    data_kiot_viet.invoice_details_2022 inv_dt INNER JOIN 
    data_kiot_viet.invoices_2022 inv
    ON  inv_dt.invoiceid = inv.id
WHERE EXISTS (SELECT NULL FROM data_kiot_viet.returns_2022 rt WHERE inv_dt.invoiceid = rt.invoiceId AND DATE(rt.createdDate) = DATE(rp_date) AND rt.status = 1)
) inv_rs
ON inv_rs.invoiceid = rt_rs.invoiceid AND inv_rs.productcode = rt_rs.productcode)
rs
GROUP BY createdDate, m_rp, y_rp, productcode, productname,branchname, sales_code;

COMMIT;

/*--------------------------------------------------!
03. Bảng kết quả
!--------------------------------------------------*/

INSERT INTO data_kiot_viet.daily_sales

WITH a AS (
SELECT createdDate, m_rp, y_rp, productcode, productname,branchname, SUM(quantity) quantity, SUM(subtotal) subtotal, sales_code 
FROM (
SELECT DISTINCT ds.* FROM data_kiot_viet.daily_sales_temp ds 
WHERE NOT EXISTS (SELECT NULL FROM data_kiot_viet.branch br WHERE ds.branchname = br.branchName 
				  AND br.id IN ('253446','116012','253441','253443','297997','253383','298145','253384','253442','253387','253410','253386','253385','152715','253444'))
	 ) ds
WHERE createdDate = rp_date
GROUP BY createdDate, m_rp, y_rp, productcode, productname,branchname, sales_code
),
invt AS (
SELECT * FROM data_kiot_viet.inventory WHERE DATE(reportingdate) = rp_date
)
SELECT DISTINCT
 CASE WHEN a.branchname = '01.TỔNG KHO 2' THEN N'TỔNG KHO 2'
WHEN a.branchname = N'1.HP.Kho Online' THEN '1.HP.Kho Online'
WHEN (a.branchname LIKE '%HP%' AND a.branchname <> N'1.HP.Kho Online') THEN N'Hải Phòng'
WHEN a.branchname LIKE '%HD%' THEN N'Hải Dương'
WHEN a.branchname LIKE '%QN%' THEN N'Quảng Ninh'
WHEN a.branchname = N'Hàng Ngoại Tỉnh (MKT)' THEN 'Hàng Ngoại Tỉnh (MKT)'
ELSE 'UNDEFINED' END Region,
    a.*,
    CASE WHEN a.productname LIKE '%CLO%' THEN 'CLO' ELSE c.categoryname_1 END nhom_hang,
    b.name,
    b.title,
    CASE WHEN b.is_terminated = '0' THEN 'Đang làm việc'
		 WHEN b.is_terminated = '1' AND DATE_FORMAT(FROM_UNIXTIME(b.terminated_date),
                '%Y-%m-%d') <= createdDate THEN 'Nghỉ việc'
		WHEN b.is_terminated = '1' AND DATE_FORMAT(FROM_UNIXTIME(b.terminated_date),
                '%Y-%m-%d') > createdDate THEN 'Đang làm việc' ELSE NULL END is_terminated,
    CASE
        WHEN b.terminated_date = '0' THEN NULL
        ELSE DATE_FORMAT(FROM_UNIXTIME(b.terminated_date),
                '%Y-%m-%d')
    END terminated_date,
    d.cost,
    (a.subtotal - d.cost*a.quantity) profit
FROM a
        LEFT JOIN
    data_hrm.employees b ON a.sales_code = b.code
		LEFT JOIN data_kiot_viet.product_categories c ON a.productcode = c.code
		LEFT JOIN invt d 
											ON a.productcode = d.productcode 
                                            AND a.branchname = d.branchname;
COMMIT;

CLOSE c_d;
END;

/*--------------------------------------------------!
Xoá ngày dữ liệu đã insert thành công vào bảng kết quả
!--------------------------------------------------*/

SET SQL_SAFE_UPDATES = 0;
DELETE FROM data_kiot_viet.data_time WHERE dayid = rp_date;
COMMIT;

END LOOP;

END