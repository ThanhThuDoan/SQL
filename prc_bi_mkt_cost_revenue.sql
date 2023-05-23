CREATE DEFINER=`root`@`%` PROCEDURE `prc_bi_mkt_cost_revenue`()
BEGIN

/*--------------------------------------------------!
01. Get data mục tiêu ngày
!--------------------------------------------------*/

SET SQL_MODE = 'NO_ENGINE_SUBSTITUTION';
SET SQL_SAFE_UPDATES = 0;

TRUNCATE TABLE data_kiot_viet.master_mkt_target_daily;

INSERT INTO data_kiot_viet.master_mkt_target_daily
WITH list_date AS (
SELECT date_field, 
DATE_FORMAT(date_field,'%Y-%m') month_field,
COUNT(date_field) OVER(PARTITION BY DATE_FORMAT(date_field,'%Y-%m')) num_day_of_month
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
WHERE MONTH(date_field) = MONTH(CURDATE())
)
SELECT list_date.*, 
	CASE WHEN loai_chi_phi IN ('b_happy','voucher','giam_gia_hang_ban','in_an','quang_cao','diem','in_an') THEN '01.Chi phí'
		ELSE '02.Doanh thu' END phan_loai,
    CASE
        WHEN loai_chi_phi = 'b_happy' THEN 'B-Happy'
        WHEN loai_chi_phi = 'voucher' THEN 'Voucher'
        WHEN loai_chi_phi = 'doanh_so' THEN 'Doanh thu'
        WHEN loai_chi_phi = 'giam_gia_hang_ban' THEN 'Giảm giá hàng bán'
        WHEN loai_chi_phi = 'in_an' THEN 'In ấn'
        WHEN loai_chi_phi = 'quang_cao' THEN 'Quảng cáo'
        WHEN loai_chi_phi = 'so_luong_kh' THEN 'Số lượng khách hàng'
        WHEN loai_chi_phi = 'diem' THEN 'Point'
        WHEN loai_chi_phi = 'in_an' THEN 'In ấn'
        ELSE NULL
    END loai_chi_phi,
ROUND(du_kien.du_kien/list_date.num_day_of_month,0) muc_tieu_ngay
FROM list_date
LEFT JOIN (SELECT thang, loai_chi_phi, du_kien FROM data_kiot_viet.master_mkt_dukien_chiphi WHERE thang IS NOT NULL) du_kien
ON list_date.month_field = du_kien.thang
ORDER BY loai_chi_phi, date_field
;
COMMIT;

/*--------------------------------------------------!
02. Get data thực tế
!--------------------------------------------------*/

TRUNCATE TABLE data_kiot_viet.bi_mkt_cost_revenue;
INSERT INTO data_kiot_viet.bi_mkt_cost_revenue

-- 1. Voucher + Point

SELECT
    DATE(createdDate_invoice) createdDate,
    method,
    SUM(amount) amount
FROM
    data_kiot_viet.payment_details pm
WHERE
    pm.method IN ('Point' , 'Voucher')
        AND YEAR(createdDate_invoice) = YEAR(CURDATE())
        AND MONTH(createdDate_invoice) = MONTH(CURDATE())
GROUP BY DATE(createdDate_invoice) , method

UNION ALL

-- 2. B-Happy

SELECT
    createdDate, 'B-Happy' method, SUM(quantity) * 50000
FROM
    data_kiot_viet.daily_sales ds
WHERE
    YEAR(ds.createdDate) = YEAR(CURDATE())
        AND MONTH(ds.createdDate) = MONTH(CURDATE())
        AND productcode = 'SP5500031901'
GROUP BY createdDate , 'B-Happy'

UNION ALL

-- 3. Giảm giá hàng bán

SELECT
DATE(inv_dt.createdDate) createdDate,
'Giảm giá hàng bán', 
ROUND(SUM(inv_dt.quantity*inv_dt.price - inv_dt.subtotal),0)
FROM
    data_kiot_viet.invoice_details_2022 inv_dt
WHERE
    YEAR(inv_dt.createdDate) = YEAR(CURDATE())
        AND MONTH(inv_dt.createdDate) = MONTH(CURDATE())
        AND inv_dt.quantity * inv_dt.price > inv_dt.subTotal
        AND NOT EXISTS( SELECT 
            NULL
        FROM
            data_kiot_viet.invoices_2022 inv
        WHERE
            inv_dt.code = inv.code
                AND YEAR(inv.createdDate) = YEAR(CURDATE())
                AND MONTH(inv.createdDate) = MONTH(CURDATE())
                AND inv.status = 2)
        AND NOT EXISTS( SELECT 
            rt_dt.*, rt.invoiceId
        FROM
            data_kiot_viet.returns_2022 rt
                INNER JOIN
            data_kiot_viet.returns_details_2022 rt_dt ON rt.code = rt_dt.code
        WHERE
            rt.status = 1
                AND rt.invoiceId = inv_dt.invoiceId)
                GROUP BY DATE(inv_dt.createdDate), 'Giảm giá hàng bán'
                
UNION ALL

-- 4. Doanh thu, Số lượng khách hàng, Số lượng hoá đơn

SELECT
    createdDate, 'Doanh thu', SUM(subtotal_f) subtotal_f
FROM
    data_kiot_viet.daily_sales_branch_all
WHERE
    YEAR(createdDate) = YEAR(CURDATE())
        AND MONTH(createdDate) = MONTH(CURDATE())
GROUP BY createdDate , 'Doanh thu'

UNION ALL

SELECT
    createdDate, 'Số lượng hoá đơn', SUM(Slg_HD) Slg_HD
FROM
    data_kiot_viet.daily_sales_branch_all
WHERE
    YEAR(createdDate) = YEAR(CURDATE())
        AND MONTH(createdDate) = MONTH(CURDATE())
GROUP BY createdDate , 'Số lượng hoá đơn'

UNION ALL

SELECT
    createdDate, 'Số lượng khách hàng', SUM(Slg_KH) Slg_KH
FROM
    data_kiot_viet.daily_sales_branch_all
WHERE
    YEAR(createdDate) = YEAR(CURDATE())
        AND MONTH(createdDate) = MONTH(CURDATE())
GROUP BY createdDate , 'Số lượng khách hàng'

UNION ALL

SELECT
	STR_TO_DATE(ngay,'%d/%m/%Y') ngay, 
    CASE WHEN loai_chi_phi = 'quang_cao' THEN 'Quảng cáo'
		WHEN loai_chi_phi = 'in_an' THEN 'In ấn' ELSE NULL END method, thuc_te 
FROM 
	data_kiot_viet.master_mkt_dukien_thucte 
WHERE 
	YEAR(STR_TO_DATE(ngay,'%d/%m/%Y')) = YEAR(CURDATE())
    AND MONTH(STR_TO_DATE(ngay,'%d/%m/%Y')) = MONTH(CURDATE());

COMMIT;

/*--------------------------------------------------!
03. Update lại mục tiêu
!--------------------------------------------------*/

BEGIN

DECLARE ch_done1 INTEGER DEFAULT 0;
DECLARE v_date1 DATE DEFAULT NULL;
DECLARE v_method1 VARCHAR(250) DEFAULT NULL;
DECLARE v_amount_d INTEGER DEFAULT NULL;

-- Cursor 1: Update mục tiêu ngày

DECLARE c_1 CURSOR FOR

SELECT 
 createdDate, method, amount
FROM
    data_kiot_viet.bi_mkt_cost_revenue;

DECLARE CONTINUE HANDLER FOR NOT FOUND SET ch_done1 = 1;

OPEN c_1;
c1loop: LOOP
FETCH NEXT FROM c_1 INTO v_date1, v_method1, v_amount_d;

IF ch_done1 = 1 
	THEN LEAVE c1loop;
END IF;

UPDATE data_kiot_viet.master_mkt_target_daily t1
LEFT JOIN (
SELECT * FROM (
SELECT date_field, month_field, num_day_of_month, loai_chi_phi, 
MAX(date_field) OVER(PARTITION BY month_field) max_month_day,
muc_tieu_ngay
FROM data_kiot_viet.master_mkt_target_daily 
WHERE loai_chi_phi = v_method1
AND date_field = v_date1
ORDER BY loai_chi_phi, date_field
) a
WHERE date_field = v_date1
) t2
ON t1.loai_chi_phi = t2.loai_chi_phi
AND t1.date_field = t2.date_field 
SET t1.muc_tieu_ngay = t1.muc_tieu_ngay + ROUND(t2.muc_tieu_ngay/DATEDIFF(t2.max_month_day,t2.date_field),0)
WHERE t1.date_field BETWEEN DATE_ADD(t2.date_field,INTERVAL 1 DAY) AND t2.max_month_day
AND t2.muc_tieu_ngay - v_amount_d > 0;
COMMIT;

END LOOP c1loop;

CLOSE c_1;
END;

/*--------------------------------------------------!
04. Bảng kết quả dự kiến/thực tế
!--------------------------------------------------*/

/*
CREATE VIEW data_kiot_viet.bi_mkt_cost_revenue_v AS
    SELECT 
        du_kien.date_field,
        du_kien.month_field,
        du_kien.num_day_of_month,
        du_kien.phan_loai,
        du_kien.loai_chi_phi,
        du_kien.muc_tieu_ngay,
        thuc_dat.createdDate,
        IFNULL(thuc_dat.amount, 0) amount
    FROM
        data_kiot_viet.master_mkt_target_daily du_kien
            LEFT JOIN
        data_kiot_viet.bi_mkt_cost_revenue thuc_dat ON du_kien.date_field = thuc_dat.createdDate
            AND du_kien.loai_chi_phi = thuc_dat.method;
COMMIT;
*/

END