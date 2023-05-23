CREATE DEFINER=`root`@`%` PROCEDURE `prc_bi_pccu_2_kpi`()
BEGIN
/*--------------------------------------------------!
01. KPI THU MUA
!--------------------------------------------------*/

TRUNCATE TABLE data_kiot_viet.kpi_thumua_report;

INSERT INTO data_kiot_viet.kpi_thumua_report
WITH ds_dt AS
(
SELECT productcode,
SUM(quantity) quantity,
SUM(subtotal) subtotal,
SUM(profit) profit
FROM data_kiot_viet.daily_sales
WHERE MONTH(createdDate) = MONTH(DATE_ADD(CURDATE(),INTERVAL -1 DAY))
AND YEAR(createdDate) = YEAR(DATE_ADD(CURDATE(),INTERVAL -1 DAY)) - 1
GROUP BY productcode
),
ds_fc AS
(
SELECT dm.ma_nganh_hang,
SUM(quantity) quantity,
SUM(subtotal) subtotal,
SUM(profit) profit
FROM data_kiot_viet.daily_sales ds
LEFT JOIN data_kiot_viet.master_dmhh dm
ON ds.productcode = dm.ma_hang
WHERE MONTH(createdDate) = MONTH(DATE_ADD(CURDATE(),INTERVAL -1 DAY))
AND YEAR(createdDate) = YEAR(DATE_ADD(CURDATE(),INTERVAL -1 DAY))
GROUP BY dm.ma_nganh_hang
)
SELECT 
dmhh.NV_TM "Nhân viên thu mua",
dmhh.ma_hang "Mã sản phẩm",
dmhh.ten_hang "Tên sản phẩm",
dmhh.loai_hh "Loại hàng hoá",
dmhh.ma_nganh_hang "Mã ngành hàng",
dmhh.ten_nganh_hang "Tên ngành hàng",
SUM(ds_dt.quantity) "Số bán",
SUM(ds_dt.subtotal) "Doanh thu",
CASE WHEN IFNULL(SUM(ds_dt.subtotal),0)  = 0 THEN 0 ELSE SUM(ds_dt.profit)/SUM(ds_dt.subtotal) END "Tỷ suất lợi nhuận",
CASE WHEN IFNULL(SUM(ds_fc.subtotal),0)  = 0 THEN 0 ELSE SUM(ds_fc.profit)/SUM(ds_fc.subtotal) END "Tỷ suất lợi nhuận ngành hàng"
FROM data_kiot_viet.master_dmhh dmhh
LEFT JOIN ds_dt
ON dmhh.ma_hang = ds_dt.productcode
LEFT JOIN ds_fc
ON dmhh.ma_nganh_hang = ds_fc.ma_nganh_hang
WHERE dmhh.ma_nganh_hang NOT IN ('HC','KM')
AND dmhh.ma_nganh_hang IS NOT NULL
AND dmhh.loai_hh NOT IN ('KM','QKM','TSCD','VTTH')
GROUP BY 
dmhh.NV_TM,
dmhh.ma_hang,
dmhh.ten_hang,
dmhh.loai_hh,
dmhh.ma_nganh_hang,
dmhh.ten_nganh_hang
ORDER BY dmhh.NV_TM DESC;
COMMIT;

/*--------------------------------------------------!
01. KPI CUNG ỨNG
!--------------------------------------------------*/

-- Bảng số lượng cửa hàng tồn = 0

TRUNCATE TABLE data_kiot_viet.inventory_curmonth_tk;
INSERT INTO data_kiot_viet.inventory_curmonth_tk
SELECT DATE_ADD(DATE(reportingDate), INTERVAL -1 DAY) reportingDate,
productCode, productName, categoryName,
branchname, onhand, cost
FROM data_kiot_viet.inventory WHERE YEAR(DATE_ADD(DATE(reportingDate), INTERVAL -1 DAY)) = YEAR(CURDATE()) - 1
AND MONTH(DATE_ADD(DATE(reportingDate), INTERVAL -1 DAY)) = MONTH(CURDATE())
AND branchname NOT IN (
'Bộ phận Kho',
'Hàng Ngoại Tỉnh (MKT)',
'Kho hàng chờ xử lý',
'Kho hàng xuất hủy',
'Phòng Giao Nhận',
'Phòng Hành Chính Nhân Sự',
'Phòng IT',
'Phòng Kinh Doanh',
'Phòng Kiểm toán',
'Phòng Kế Toán',
'Phòng Marketing',
'Phòng Setup',
'Phòng Thu Mua',
'Phòng Truyền Thông - Đào Tạo',
'Phòng Điều Hành',
'Quà KM',
'02.Tổng công ty'
);
COMMIT;

-- Bảng kết quả:

TRUNCATE TABLE data_kiot_viet.kpi_cungung_report;
INSERT INTO data_kiot_viet.kpi_cungung_report
WITH date_tb AS (
SELECT date_field,
DATE_FORMAT(date_field,'%Y-%m') month_field,
COUNT(date_field) OVER(PARTITION BY DATE_FORMAT(date_field,'%Y-%m')) num_day_of_month,
CONCAT('Tuần ',
            FLOOR((DAYOFMONTH(date_field) - 1) / 7) + 1) week_field,
COUNT(date_field) OVER(PARTITION BY CONCAT('Tuần ',
            FLOOR((DAYOFMONTH(date_field) - 1) / 7) + 1)) num_day_of_week
FROM
(
    SELECT
        MAKEDATE(YEAR(NOW()),1) +
        INTERVAL (MONTH(NOW())-2) MONTH +
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
WHERE MONTH(date_field) = MONTH(DATE_ADD(CURDATE(),INTERVAL -1 DAY))
AND YEAR(date_field) = YEAR(DATE_ADD(CURDATE(),INTERVAL -1 DAY))
),
dmhh AS (
SELECT DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -1 DAY),'%Y-%m') month_field,
a.* FROM data_kiot_viet.master_dmhh a
),
ton_ko AS (
SELECT invt_cur.reportingDate, invt_cur.productcode, COUNT(DISTINCT invt_cur.branchname) slg_ch_ton_0 
FROM data_kiot_viet.inventory_curmonth_tk invt_cur
WHERE invt_cur.branchname <> '01.TỔNG KHO 2'
AND invt_cur.onhand = 0
GROUP BY invt_cur.reportingDate, invt_cur.productcode
),
ds AS (
SELECT createdDate,productcode, SUM(subtotal) subtotal FROM data_kiot_viet.daily_sales WHERE YEAR(createdDate) = YEAR(CURDATE()) - 1
AND MONTH(createdDate) = MONTH(CURDATE())
GROUP BY createdDate,productcode
),
gv AS
(
SELECT invt_2.reportingDate, invt_2.productcode, invt_2.cost FROM data_kiot_viet.inventory_curmonth_tk invt_2
WHERE invt_2.branchname = '01.TỔNG KHO 2'
)
SELECT date_tb.date_field,
date_tb.month_field,
dmhh.nv_cu,
dmhh.ma_hang,
dmhh.ten_hang,
dmhh.loai_hh,
dmhh.Ma_nganh_hang,
dmhh.Ten_nganh_hang,
IFNULL(ton_ko.slg_ch_ton_0,0) slg_ch_ton_0,
IFNULL(ds.subtotal,0) subtotal,
gv.cost,
gb.basePrice
FROM dmhh
INNER JOIN date_tb
ON  dmhh.month_field = date_tb.month_field
LEFT JOIN ton_ko
ON date_tb.date_field = ton_ko.reportingDate
AND dmhh.ma_hang = ton_ko.productcode
LEFT JOIN ds
ON date_tb.date_field = ds.createdDate
AND dmhh.ma_hang = ds.productcode
LEFT JOIN gv
ON date_tb.date_field = gv.reportingDate
AND dmhh.ma_hang = gv.productcode
LEFT JOIN data_kiot_viet.product gb
ON dmhh.ma_hang = gb.code
WHERE dmhh.loai_hh NOT IN ('VTTH','KM','TSCD','QKM');
COMMIT;
END