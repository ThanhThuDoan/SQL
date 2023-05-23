CREATE DEFINER=`root`@`%` PROCEDURE `prc_nhu_cau_hh_ver2`()
BEGIN

/*--------------------------------------------------!
2) OOS
!--------------------------------------------------*/
TRUNCATE TABLE data_kiot_viet.invt_90days_onhand0;

INSERT INTO data_kiot_viet.invt_90days_onhand0
WITH aa AS (
SELECT invt.productcode, invt.branchName, DATE(invt.reportingDate) reportingDate
FROM data_kiot_viet.inventory invt
LEFT JOIN 
(
SELECT productcode, DATE(MIN(createdDate)) createdDate FRom data_kiot_viet.Purchase_order_details GROUP BY productcode
) prd
ON invt.productcode = prd.productcode
LEFT JOIN  data_kiot_viet.master_leadtime_kho kho
ON invt.branchname = kho.ma_kho_nhan
WHERE DATE_ADD(DATE(invt.reportingDate), INTERVAL -1 DAY) BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY)
AND invt.onhand = 0
AND DATE_ADD(DATE(invt.reportingDate), INTERVAL -1 DAY) >= DATE_ADD(IFNULL(prd.createdDate,DATE(invt.reportingDate)), INTERVAL IFNULL(kho.tong_leadtime_kho,0) DAY)
)
SELECT productCode, branchName, COUNT(DISTINCT reportingDate) num_of_date
FROM aa
WHERE branchName NOT IN
(
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
'02.Tổng công ty',
'1.HP.Kho Online',
'01.TỔNG KHO 2'
)
GROUP BY productCode, branchName;

COMMIT;

/*--------------------------------------------------!
3) Master dmhh ver 2
!--------------------------------------------------*/

TRUNCATE TABLE data_kiot_viet.master_dmhh_ver2;

INSERT INTO data_kiot_viet.master_dmhh_ver2
SELECT dmhh.ma_hang,
dmhh.ten_hang,
DATE(prd.createdDate) ngay_tao_sp,
dmhh.brand,
dmhh.loai_hh,
dmhh.mua_vu,
dmhh.ma_ncc,
dmhh.ten_ncc,
dmhh.ncc_lendon,
dmhh.ma_nganh_hang,
dmhh.ten_nganh_hang,
dmhh.ma_nhom_nganh_hang,
dmhh.ten_nhom_nganh_hang,
dmhh.ma_nhom_sp,
dmhh.ten_nhom_sp,
dmhh.ma_function,
dmhh.ten_function,
dmhh.ngay_ton_kho_trung_bay,
dmhh.moq_po,
dmhh.moq_to,
rp_631.range_review,
dmhh.nv_cu,
dmhh.nv_tm,
dmhh.gioi_tinh,
dmhh.do_tuoi,
dmhh.tan_suat
FROM data_kiot_viet.master_dmhh dmhh
LEFT JOIN data_kiot_viet.auto_reporting_631 rp_631
ON dmhh.ma_hang = rp_631.ma_hang
LEFT JOIN
data_kiot_viet.product prd
ON dmhh.ma_hang = prd.code
;
COMMIT;

/*--------------------------------------------------!
3) Nhu cầu hàng hoá ver 2
!--------------------------------------------------*/
BEGIN 

DECLARE cur_dow BIGINT;
DECLARE cur_date Date;
SET cur_dow = DAYOFWEEK(CURDATE());
SET cur_date = CURDATE();
IF cur_dow IN (5,6,7) OR cur_date BETWEEN str_to_date('2022-08-30','%Y-%m-%d') AND str_to_date('2022-09-04','%Y-%m-%d') THEN
TRUNCATE TABLE data_kiot_viet.order_nchh_temp_ver2;
SET sql_mode = 'NO_ENGINE_SUBSTITUTION';
INSERT INTO data_kiot_viet.order_nchh_temp_ver2
WITH tf AS ( 
SELECT
    SUM(sendquantity) sendquantity, tobranchname, productcode
FROM
    data_kiot_viet.transfer
GROUP BY tobranchname , productcode
),
tf_20 AS
(
SELECT MIN(DATE(tf_120.receivedDate)) dispatchedDate, tf_120.productCode, br.branchName
FROM data_kiot_viet.transfer_120_days tf_120
LEFT JOIN data_kiot_viet.branch br
ON tf_120.toBranchId = br.id
WHERE tf_120.status = 3
GROUP BY tf_120.productCode, br.branchName
)
SELECT 
sysdate() event_date,
	spl.productid,
    spl.productcode,
    spl.productname,
    spl.branchname,
    dmhh.Loai_hh,
    dmhh.Ma_nganh_hang,
    dmhh.Ten_nganh_hang,
    dmhh.Ten_Function,
    dmhh.Ma_NCC,
    dmhh.Ten_NCC,
    dmhh.NCC_lendon,
    dmhh.range_review,
    dmhh.ngay_tao_sp,
    tf_20.dispatchedDate transfer_120days,
    CASE WHEN dmhh.ngay_tao_sp BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN
	CASE WHEN tf_20.dispatchedDate IS NULL THEN 0
	WHEN tf_20.dispatchedDate IS NOT NULL THEN DATEDIFF(DATE_ADD(CURDATE(), INTERVAL -1 DAY),tf_20.dispatchedDate) END
	WHEN dmhh.ngay_tao_sp NOT BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN 90
	ELSE NULL END num_day_sales,
	IFNULL(oos.num_of_date,0) AS num_date_onhand0,
	spl.cost gia_von,
    CASE WHEN spl.quantity_f < 0 THEN 0 ELSE spl.quantity_f END quantity_f,
    CAST(spl.onhand AS DECIMAL) onhand,
    spl.onhand_amount,
    IFNULL(tf.sendquantity, 0) sendquantity,
    dmhh.NV_CU,
    SUM(spl.quantity_f) OVER(PARTITION BY spl.productcode) quantity_f_all,
    CASE WHEN spl.quantity_f < 0 THEN 0 ELSE spl.quantity_f/(CASE WHEN (
	CASE WHEN dmhh.ngay_tao_sp BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN
	CASE WHEN tf_20.dispatchedDate IS NULL THEN 0
	WHEN tf_20.dispatchedDate IS NOT NULL THEN DATEDIFF(DATE_ADD(CURDATE(), INTERVAL -1 DAY),tf_20.dispatchedDate) END
	WHEN dmhh.ngay_tao_sp NOT BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN 90
	ELSE NULL END
    - IFNULL(oos.num_of_date,0)) <= 0 THEN 1
	ELSE
    (
    CASE WHEN dmhh.ngay_tao_sp BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN
	CASE WHEN tf_20.dispatchedDate IS NULL THEN 0
	WHEN tf_20.dispatchedDate IS NOT NULL THEN DATEDIFF(DATE_ADD(CURDATE(), INTERVAL -1 DAY),tf_20.dispatchedDate) END
	WHEN dmhh.ngay_tao_sp NOT BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN 90
	ELSE NULL END
    - IFNULL(oos.num_of_date,0)) END) END DailySales_30,
    IFNULL(dmhh.Ngay_ton_kho_trung_bay + 2, 9) Ngay_ton_kho_trung_bay,
    CASE WHEN dmhh.range_review IN ('RANGE','OUT RANGE') THEN 
    FLOOR(data_kiot_viet.ps_trung_bay(dmhh.range_review, IFNULL(dmhh.Ngay_ton_kho_trung_bay + 2,9),CASE WHEN spl.quantity_f < 0 THEN 0 ELSE spl.quantity_f/(CASE WHEN (
    CASE WHEN dmhh.ngay_tao_sp BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN
	CASE WHEN tf_20.dispatchedDate IS NULL THEN 0
	WHEN tf_20.dispatchedDate IS NOT NULL THEN DATEDIFF(DATE_ADD(CURDATE(), INTERVAL -1 DAY),tf_20.dispatchedDate) END
	WHEN dmhh.ngay_tao_sp NOT BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN 90
	ELSE NULL END 
    - IFNULL(oos.num_of_date,0)) <= 0 THEN 1
	ELSE
    (
	CASE WHEN dmhh.ngay_tao_sp BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN
	CASE WHEN tf_20.dispatchedDate IS NULL THEN 0
	WHEN tf_20.dispatchedDate IS NOT NULL THEN DATEDIFF(DATE_ADD(CURDATE(), INTERVAL -1 DAY),tf_20.dispatchedDate) END
	WHEN dmhh.ngay_tao_sp NOT BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN 90
	ELSE NULL END
    - IFNULL(oos.num_of_date,0)) END) END)) 
    ELSE ceiling(data_kiot_viet.ps_trung_bay(dmhh.range_review, IFNULL(dmhh.Ngay_ton_kho_trung_bay + 2,9),CASE WHEN spl.quantity_f < 0 THEN 0 ELSE spl.quantity_f/(CASE WHEN (
    CASE WHEN dmhh.ngay_tao_sp BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN
	CASE WHEN tf_20.dispatchedDate IS NULL THEN 0
	WHEN tf_20.dispatchedDate IS NOT NULL THEN DATEDIFF(DATE_ADD(CURDATE(), INTERVAL -1 DAY),tf_20.dispatchedDate) END
	WHEN dmhh.ngay_tao_sp NOT BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN 90
	ELSE NULL END 
    - IFNULL(oos.num_of_date,0)) <= 0 THEN 1
	ELSE
    (
    CASE WHEN dmhh.ngay_tao_sp BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN
	CASE WHEN tf_20.dispatchedDate IS NULL THEN 0
	WHEN tf_20.dispatchedDate IS NOT NULL THEN DATEDIFF(DATE_ADD(CURDATE(), INTERVAL -1 DAY),tf_20.dispatchedDate) END
	WHEN dmhh.ngay_tao_sp NOT BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN 90
	ELSE NULL END
    - IFNULL(oos.num_of_date,0)) END) END))  end  PS_trung_bay,
    ncc.Leadtime_giao_hang,
    kho.tong_leadtime_kho,
    ncc.Tan_suat_dat_hang_FQPO,
    ncc.Tan_suat_chia_hang_FQTO,
    ncc.lich_dat_hang,
    dmhh.MOQ_PO,
    dmhh.MOQ_TO
FROM 
    data_kiot_viet.Supply_90days spl
        LEFT JOIN
    data_kiot_viet.master_dmhh_ver2 dmhh ON spl.productcode = TRIM(dmhh.ma_hang)
		LEFT JOIN
	data_kiot_viet.master_leadtime_ncc ncc ON dmhh.ma_ncc = TRIM(ncc.ma_ncc)
        LEFT JOIN
    tf ON spl.productcode = tf.productCode
        AND spl.branchname = tf.TobranchName
		LEFT JOIN 
	tf_20 ON spl.productcode = tf_20.productCode AND spl.branchname = tf_20.branchName
		LEFT JOIN
	data_kiot_viet.master_leadtime_kho kho ON spl.branchName = kho.ma_kho_nhan
		LEFT JOIN 
	data_kiot_viet.invt_90days_onhand0 oos ON spl.productcode = oos.productCode AND spl.branchname = oos.branchName
    WHERE spl.branchname NOT IN (N'1.HP.50 Lạch Tray (Q. Ngô Quyền)',N'1.HP.Kho Online',N'01.TỔNG KHO 2',
							     N'6.QN.60 Trần Nhân Tông (TP. Uông Bí)','Kho hàng xuất hủy') -- bsung 12.09.2022
;
    COMMIT;
ELSE
SET sql_mode = 'NO_ENGINE_SUBSTITUTION';
TRUNCATE TABLE data_kiot_viet.order_nchh_temp_ver2;
INSERT INTO data_kiot_viet.order_nchh_temp_ver2
WITH tf AS ( 
SELECT
    SUM(sendquantity) sendquantity, tobranchname, productcode
FROM
    data_kiot_viet.transfer
GROUP BY tobranchname , productcode
),
tf_20 AS
(
SELECT MIN(DATE(tf_120.receivedDate)) dispatchedDate, tf_120.productCode, br.branchName
FROM data_kiot_viet.transfer_120_days tf_120
LEFT JOIN data_kiot_viet.branch br
ON tf_120.toBranchId = br.id
WHERE tf_120.status = 3
GROUP BY tf_120.productCode, br.branchName
)
SELECT 
sysdate() event_date,
	spl.productid,
    spl.productcode,
    spl.productname,
    spl.branchname,
    dmhh.Loai_hh,
    dmhh.Ma_nganh_hang,
    dmhh.Ten_nganh_hang,
    dmhh.Ten_Function,
    dmhh.Ma_NCC,
    dmhh.Ten_NCC,
    dmhh.NCC_lendon,
    dmhh.range_review,
    dmhh.ngay_tao_sp,
    tf_20.dispatchedDate transfer_120days,
    CASE WHEN dmhh.ngay_tao_sp BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN
	CASE WHEN tf_20.dispatchedDate IS NULL THEN 0
	WHEN tf_20.dispatchedDate IS NOT NULL THEN DATEDIFF(DATE_ADD(CURDATE(), INTERVAL -1 DAY),tf_20.dispatchedDate) END
	WHEN dmhh.ngay_tao_sp NOT BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN 90
	ELSE NULL END num_day_sales,
	IFNULL(oos.num_of_date,0) AS num_date_onhand0,
	spl.cost gia_von,
    CASE WHEN spl.quantity_f < 0 THEN 0 ELSE spl.quantity_f END quantity_f,
    CAST(spl.onhand AS DECIMAL) onhand,
    spl.onhand_amount,
    IFNULL(tf.sendquantity, 0) sendquantity,
    dmhh.NV_CU,
    SUM(spl.quantity_f) OVER(PARTITION BY spl.productcode) quantity_f_all,
    CASE WHEN spl.quantity_f < 0 THEN 0 ELSE spl.quantity_f/(CASE WHEN (
    CASE WHEN dmhh.ngay_tao_sp BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN
	CASE WHEN tf_20.dispatchedDate IS NULL THEN 0
	WHEN tf_20.dispatchedDate IS NOT NULL THEN DATEDIFF(DATE_ADD(CURDATE(), INTERVAL -1 DAY),tf_20.dispatchedDate) END
	WHEN dmhh.ngay_tao_sp NOT BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN 90
	ELSE NULL END 
    - IFNULL(oos.num_of_date,0)) <= 0 THEN 1
	ELSE
    (
    CASE WHEN dmhh.ngay_tao_sp BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN
	CASE WHEN tf_20.dispatchedDate IS NULL THEN 0
	WHEN tf_20.dispatchedDate IS NOT NULL THEN DATEDIFF(DATE_ADD(CURDATE(), INTERVAL -1 DAY),tf_20.dispatchedDate) END
	WHEN dmhh.ngay_tao_sp NOT BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN 90
	ELSE NULL END
    - IFNULL(oos.num_of_date,0)) END) END DailySales_30,
    IFNULL(dmhh.Ngay_ton_kho_trung_bay, 7) Ngay_ton_kho_trung_bay,
    CASE WHEN dmhh.range_review IN ('RANGE','OUT RANGE') THEN 
    FLOOR(data_kiot_viet.ps_trung_bay(dmhh.range_review, IFNULL(dmhh.Ngay_ton_kho_trung_bay,7),CASE WHEN spl.quantity_f < 0 THEN 0 ELSE spl.quantity_f/(CASE WHEN (
    CASE WHEN dmhh.ngay_tao_sp BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN
	CASE WHEN tf_20.dispatchedDate IS NULL THEN 0
	WHEN tf_20.dispatchedDate IS NOT NULL THEN DATEDIFF(DATE_ADD(CURDATE(), INTERVAL -1 DAY),tf_20.dispatchedDate) END
	WHEN dmhh.ngay_tao_sp NOT BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN 90
	ELSE NULL END
    - IFNULL(oos.num_of_date,0)) <= 0 THEN 1
	ELSE
    (
    CASE WHEN dmhh.ngay_tao_sp BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN
	CASE WHEN tf_20.dispatchedDate IS NULL THEN 0
	WHEN tf_20.dispatchedDate IS NOT NULL THEN DATEDIFF(DATE_ADD(CURDATE(), INTERVAL -1 DAY),tf_20.dispatchedDate) END
	WHEN dmhh.ngay_tao_sp NOT BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN 90
	ELSE NULL END 
    - IFNULL(oos.num_of_date,0)) END) END)) 
    ELSE ceiling(data_kiot_viet.ps_trung_bay(dmhh.range_review, IFNULL(dmhh.Ngay_ton_kho_trung_bay,7),CASE WHEN spl.quantity_f < 0 THEN 0 ELSE spl.quantity_f/(CASE WHEN (
    CASE WHEN dmhh.ngay_tao_sp BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN
	CASE WHEN tf_20.dispatchedDate IS NULL THEN 0
	WHEN tf_20.dispatchedDate IS NOT NULL THEN DATEDIFF(DATE_ADD(CURDATE(), INTERVAL -1 DAY),tf_20.dispatchedDate) END
	WHEN dmhh.ngay_tao_sp NOT BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN 90
	ELSE NULL END
    - IFNULL(oos.num_of_date,0)) <= 0 THEN 1
	ELSE
    (
    CASE WHEN dmhh.ngay_tao_sp BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN
	CASE WHEN tf_20.dispatchedDate IS NULL THEN 0
	WHEN tf_20.dispatchedDate IS NOT NULL THEN DATEDIFF(DATE_ADD(CURDATE(), INTERVAL -1 DAY),tf_20.dispatchedDate) END
	WHEN dmhh.ngay_tao_sp NOT BETWEEN DATE_ADD(CURDATE(), INTERVAL -91 DAY) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN 90
	ELSE NULL END
    - IFNULL(oos.num_of_date,0)) END) END))  end  PS_trung_bay,
    ncc.Leadtime_giao_hang,
    kho.tong_leadtime_kho,
    ncc.Tan_suat_dat_hang_FQPO,
    ncc.Tan_suat_chia_hang_FQTO,
    ncc.lich_dat_hang,
    dmhh.MOQ_PO,
    dmhh.MOQ_TO
FROM 
    data_kiot_viet.Supply_90days spl
        LEFT JOIN
    data_kiot_viet.master_dmhh_ver2 dmhh ON spl.productcode = TRIM(dmhh.ma_hang)
		LEFT JOIN
	data_kiot_viet.master_leadtime_ncc ncc ON dmhh.ma_ncc = TRIM(ncc.ma_ncc)
        LEFT JOIN
    tf ON spl.productcode = tf.productCode
        AND spl.branchname = tf.TobranchName
		LEFT JOIN 
	tf_20 ON spl.productcode = tf_20.productCode AND spl.branchname = tf_20.branchName
		LEFT JOIN
	data_kiot_viet.master_leadtime_kho kho ON spl.branchName = kho.ma_kho_nhan
		LEFT JOIN 
	data_kiot_viet.invt_90days_onhand0 oos ON spl.productcode = oos.productCode AND spl.branchname = oos.branchName
    WHERE spl.branchname NOT IN (N'1.HP.50 Lạch Tray (Q. Ngô Quyền)',N'1.HP.Kho Online',N'01.TỔNG KHO 2',
							     N'6.QN.60 Trần Nhân Tông (TP. Uông Bí)','Kho hàng xuất hủy') -- bsung 12.09.2022
;
COMMIT;
    END IF;
/*--------------------------------------------------!
02. BẢNG KẾT QUẢ
!--------------------------------------------------*/

SET sql_mode = 'NO_ENGINE_SUBSTITUTION';
TRUNCATE TABLE data_kiot_viet.order_nchh_ver2;
INSERT INTO data_kiot_viet.order_nchh_ver2
WITH tb AS (
SELECT 
    nchh.*,
    IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang) kho_giao_hang,
    ROUND(IFNULL(MIN_TK(nchh.range_review,
                            IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang),
                            nchh.PS_trung_bay,
                            nchh.DailySales_30,
                            nchh.tong_leadtime_kho,
                            nchh.Tan_suat_chia_hang_FQTO,
                            nchh.leadtime_giao_hang,
                            nchh.Tan_suat_dat_hang_FQPO),
                    0),
            0) min,
    CEILING(CASE
                WHEN
                    ROUND(MIN_TK(nchh.range_review,
                                    IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang),
                                    nchh.PS_trung_bay,
                                    nchh.DailySales_30,
                                    nchh.tong_leadtime_kho,
                                    nchh.Tan_suat_chia_hang_FQTO,
                                    nchh.leadtime_giao_hang,
                                    nchh.Tan_suat_dat_hang_FQPO),
                            0) <> 0
                        AND IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang) = N'01.TỔNG KHO 2'
                        AND nchh.productcode NOT IN (select product_code from data_kiot_viet.master_max where product_code IS NOT NULL)
                THEN
                    CEILING(ROUND(MIN_TK(nchh.range_review,
                                            IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang),
                                            nchh.PS_trung_bay,
                                            nchh.DailySales_30,
                                            nchh.tong_leadtime_kho,
                                            nchh.Tan_suat_chia_hang_FQTO,
                                            nchh.leadtime_giao_hang,
                                            nchh.Tan_suat_dat_hang_FQPO),
                                    0) + nchh.DailySales_30 * nchh.Tan_suat_chia_hang_FQTO)
                WHEN
                    ROUND(MIN_TK(nchh.range_review,
                                    IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang),
                                    nchh.PS_trung_bay,
                                    nchh.DailySales_30,
                                    nchh.tong_leadtime_kho,
                                    nchh.Tan_suat_chia_hang_FQTO,
                                    nchh.leadtime_giao_hang,
                                    nchh.Tan_suat_dat_hang_FQPO),
                            0) <> 0
                        AND IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang) = 'CH'
                        AND nchh.productcode IN (select product_code from data_kiot_viet.master_max where product_code IS NOT NULL)
                THEN
                    CEILING(ROUND(MIN_TK(nchh.range_review,
                                            IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang),
                                            nchh.PS_trung_bay,
                                            nchh.DailySales_30,
                                            nchh.tong_leadtime_kho,
                                            nchh.Tan_suat_chia_hang_FQTO,
                                            nchh.leadtime_giao_hang,
                                            nchh.Tan_suat_dat_hang_FQPO),
                                    0) + nchh.DailySales_30 * nchh.Tan_suat_dat_hang_FQPO)
                                    WHEN
                    ROUND(MIN_TK(nchh.range_review,
                                    IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang),
                                    nchh.PS_trung_bay,
                                    nchh.DailySales_30,
                                    nchh.tong_leadtime_kho,
                                    nchh.Tan_suat_chia_hang_FQTO,
                                    nchh.leadtime_giao_hang,
                                    nchh.Tan_suat_dat_hang_FQPO),
                            0) <> 0
                        AND IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang) = N'01.TỔNG KHO 2'
                        AND nchh.productcode IN (select product_code from data_kiot_viet.master_max where product_code IS NOT NULL)
                THEN 1
                WHEN
                    ROUND(MIN_TK(nchh.range_review,
                                    IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang),
                                    nchh.PS_trung_bay,
                                    nchh.DailySales_30,
                                    nchh.tong_leadtime_kho,
                                    nchh.Tan_suat_chia_hang_FQTO,
                                    nchh.leadtime_giao_hang,
                                    nchh.Tan_suat_dat_hang_FQPO),
                            0) <> 0
                        AND IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang) = 'CH'
                        AND nchh.productcode IN (select product_code from data_kiot_viet.master_max where product_code IS NOT NULL)
                THEN 1
                ELSE 0
            END) max,
    CASE
        WHEN
            (nchh.onhand + nchh.sendquantity) < ROUND(MIN_TK(nchh.range_review,
                            IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang),
                            nchh.PS_trung_bay,
                            nchh.DailySales_30,
                            nchh.tong_leadtime_kho,
                            nchh.Tan_suat_chia_hang_FQTO,
                            nchh.leadtime_giao_hang,
                            nchh.Tan_suat_dat_hang_FQPO),
                    0)
        THEN
            CEILING(CASE
                WHEN
                    ROUND(MIN_TK(nchh.range_review,
                                    IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang),
                                    nchh.PS_trung_bay,
                                    nchh.DailySales_30,
                                    nchh.tong_leadtime_kho,
                                    nchh.Tan_suat_chia_hang_FQTO,
                                    nchh.leadtime_giao_hang,
                                    nchh.Tan_suat_dat_hang_FQPO),
                            0) <> 0
                        AND IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang) = N'01.TỔNG KHO 2'
                        AND nchh.productcode NOT IN (select product_code from data_kiot_viet.master_max where product_code IS NOT NULL)
                THEN
                    CEILING(ROUND(MIN_TK(nchh.range_review,
                                            IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang),
                                            nchh.PS_trung_bay,
                                            nchh.DailySales_30,
                                            nchh.tong_leadtime_kho,
                                            nchh.Tan_suat_chia_hang_FQTO,
                                            nchh.leadtime_giao_hang,
                                            nchh.Tan_suat_dat_hang_FQPO),
                                    0) + nchh.DailySales_30 * nchh.Tan_suat_chia_hang_FQTO)
                WHEN
                    ROUND(MIN_TK(nchh.range_review,
                                    IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang),
                                    nchh.PS_trung_bay,
                                    nchh.DailySales_30,
                                    nchh.tong_leadtime_kho,
                                    nchh.Tan_suat_chia_hang_FQTO,
                                    nchh.leadtime_giao_hang,
                                    nchh.Tan_suat_dat_hang_FQPO),
                            0) <> 0
                        AND IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang) = 'CH'
                        AND nchh.productcode IN (select product_code from data_kiot_viet.master_max where product_code IS NOT NULL)
                THEN
                    CEILING(ROUND(MIN_TK(nchh.range_review,
                                            IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang),
                                            nchh.PS_trung_bay,
                                            nchh.DailySales_30,
                                            nchh.tong_leadtime_kho,
                                            nchh.Tan_suat_chia_hang_FQTO,
                                            nchh.leadtime_giao_hang,
                                            nchh.Tan_suat_dat_hang_FQPO),
                                    0) + nchh.DailySales_30 * nchh.Tan_suat_dat_hang_FQPO)
                                    WHEN
                    ROUND(MIN_TK(nchh.range_review,
                                    IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang),
                                    nchh.PS_trung_bay,
                                    nchh.DailySales_30,
                                    nchh.tong_leadtime_kho,
                                    nchh.Tan_suat_chia_hang_FQTO,
                                    nchh.leadtime_giao_hang,
                                    nchh.Tan_suat_dat_hang_FQPO),
                            0) <> 0
                        AND IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang) = N'01.TỔNG KHO 2'
                        AND nchh.productcode IN (select product_code from data_kiot_viet.master_max where product_code IS NOT NULL)
                THEN 1
                WHEN
                    ROUND(MIN_TK(nchh.range_review,
                                    IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang),
                                    nchh.PS_trung_bay,
                                    nchh.DailySales_30,
                                    nchh.tong_leadtime_kho,
                                    nchh.Tan_suat_chia_hang_FQTO,
                                    nchh.leadtime_giao_hang,
                                    nchh.Tan_suat_dat_hang_FQPO),
                            0) <> 0
                        AND IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang) = 'CH'
                        AND nchh.productcode IN (select product_code from data_kiot_viet.master_max where product_code IS NOT NULL)
                THEN 1
                ELSE 0
            END) - (nchh.onhand + nchh.sendquantity)
        ELSE 0
    END goi_y_dat_hang
FROM
    data_kiot_viet.order_nchh_temp_ver2 nchh
        LEFT JOIN
    (SELECT * FROM data_kiot_viet.master_transfer WHERE (product_code IS NULL OR product_code = '')) tf1 ON nchh.branchName = tf1.chi_nhanh
        AND nchh.ma_ncc = tf1.ma_ncc
        LEFT JOIN
    (SELECT * FROM data_kiot_viet.master_transfer WHERE (product_code <> '' OR product_code IS NOT NULL)) tf2 ON nchh.productcode = tf2.product_code
		AND nchh.branchName = tf2.chi_nhanh
        AND nchh.ma_ncc = tf2.ma_ncc
        )
        SELECT DISTINCT tb.*,
CASE 
	WHEN tb.Kho_giao_hang = N'01.TỔNG KHO 2' 
		AND tb.goi_y_dat_hang > tb.MOQ_TO 
			THEN tb.goi_y_dat_hang
	WHEN tb.Kho_giao_hang = N'01.TỔNG KHO 2' 
		AND tb.goi_y_dat_hang >= 1/2*tb.MOQ_TO 
        AND tb.goi_y_dat_hang <= tb.MOQ_TO 
			THEN tb.MOQ_TO
	WHEN tb.Kho_giao_hang = 'CH' AND tb.goi_y_dat_hang >= 1/2*tb.MOQ_PO AND tb.goi_y_dat_hang <= tb.MOQ_PO THEN tb.MOQ_PO
    WHEN tb.Kho_giao_hang = 'CH' AND tb.goi_y_dat_hang > tb.MOQ_PO  THEN tb.goi_y_dat_hang
ELSE 0 END so_dat_hang_chot,
CASE 
	WHEN tb.Kho_giao_hang = N'01.TỔNG KHO 2' AND (CASE 
	WHEN tb.Kho_giao_hang = N'01.TỔNG KHO 2' 
		AND tb.goi_y_dat_hang > tb.MOQ_TO 
			THEN tb.goi_y_dat_hang
	WHEN tb.Kho_giao_hang = N'01.TỔNG KHO 2' 
		AND tb.goi_y_dat_hang >= 1/2*tb.MOQ_TO 
        AND tb.goi_y_dat_hang <= tb.MOQ_TO 
			THEN tb.MOQ_TO
	WHEN tb.Kho_giao_hang = 'CH' AND tb.goi_y_dat_hang >= 1/2*tb.MOQ_PO AND tb.goi_y_dat_hang <= tb.MOQ_PO THEN tb.MOQ_PO
    WHEN tb.Kho_giao_hang = 'CH' AND tb.goi_y_dat_hang > tb.MOQ_PO  THEN tb.goi_y_dat_hang
ELSE 0 END) > 0 THEN 'Đơn chia TO-SO'
WHEN tb.Kho_giao_hang = 'CH' AND (CASE 
	WHEN tb.Kho_giao_hang = N'01.TỔNG KHO 2' 
		AND tb.goi_y_dat_hang > tb.MOQ_TO 
			THEN tb.goi_y_dat_hang
	WHEN tb.Kho_giao_hang = N'01.TỔNG KHO 2' 
		AND tb.goi_y_dat_hang >= 1/2*tb.MOQ_TO 
        AND tb.goi_y_dat_hang <= tb.MOQ_TO 
			THEN tb.MOQ_TO
	WHEN tb.Kho_giao_hang = 'CH' AND tb.goi_y_dat_hang >= 1/2*tb.MOQ_PO AND  tb.goi_y_dat_hang <= tb.MOQ_PO THEN tb
.MOQ_PO
    WHEN tb.Kho_giao_hang = 'CH' AND tb.goi_y_dat_hang > tb.MOQ_PO  THEN tb.goi_y_dat_hang
ELSE 0 END) > 0 THEN 'Đơn đặt PO' ELSE NULL END
    loai_don FROM tb
    WHERE tb.productcode NOT IN ('SP5500031831','SP5500032742', 'SP5500031370','SP5500031718','SP5500031814','SP5500032410','SP5500032516','SP5500032517','SP5500032517','SP5500032815','SP5500033198','SP5500033490')
    ;
    COMMIT;
END;
END