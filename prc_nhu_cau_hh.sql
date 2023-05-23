CREATE DEFINER=`root`@`%` PROCEDURE `prc_nhu_cau_hh`()
BEGIN

/*--------------------------------------------------!
Nhu cầu hàng hoá
!--------------------------------------------------*/

BEGIN 

DECLARE cur_dow BIGINT;
DECLARE cur_date Date;
SET cur_dow = DAYOFWEEK(CURDATE());
SET cur_date = CURDATE();
IF cur_dow IN (5,6,7) OR cur_date BETWEEN str_to_date('2022-12-24','%Y-%m-%d') AND str_to_date('2022-12-31','%Y-%m-%d') 
THEN
TRUNCATE TABLE data_kiot_viet.order_nchh_temp;
SET sql_mode = 'NO_ENGINE_SUBSTITUTION';
INSERT INTO data_kiot_viet.order_nchh_temp
WITH tf AS ( 
SELECT
    SUM(sendquantity) sendquantity, tobranchname, productcode
FROM
    data_kiot_viet.transfer
GROUP BY tobranchname , productcode
)
SELECT sysdate() event_date,
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
    spl.cost gia_von,
    spl.quantity_f,
    CAST(spl.onhand AS DECIMAL) onhand,
    spl.onhand_amount,
    IFNULL(tf.sendquantity, 0) sendquantity,
    dmhh.NV_CU,
    SUM(spl.quantity_f) OVER(PARTITION BY spl.productcode) quantity_f_all,
    spl.quantity_f / 90 DailySales_30,
    CASE WHEN dmhh.Ngay_ton_kho_trung_bay = 0 THEN 0 WHEN dmhh.Ngay_ton_kho_trung_bay >= 10 THEN dmhh.Ngay_ton_kho_trung_bay ELSE IFNULL(dmhh.Ngay_ton_kho_trung_bay + 5, 10) END Ngay_ton_kho_trung_bay,
    CASE WHEN dmhh.range_review IN ('RANGE','OUT RANGE') THEN 
    FLOOR(data_kiot_viet.ps_trung_bay(dmhh.range_review, CASE WHEN dmhh.Ngay_ton_kho_trung_bay = 0 THEN 0 WHEN dmhh.Ngay_ton_kho_trung_bay >= 10 THEN dmhh.Ngay_ton_kho_trung_bay ELSE IFNULL(dmhh.Ngay_ton_kho_trung_bay + 5, 10) END,spl.quantity_f / 90)) 
    ELSE ceiling(data_kiot_viet.ps_trung_bay(dmhh.range_review, CASE WHEN dmhh.Ngay_ton_kho_trung_bay = 0 THEN 0 WHEN dmhh.Ngay_ton_kho_trung_bay >= 10 THEN dmhh.Ngay_ton_kho_trung_bay ELSE IFNULL(dmhh.Ngay_ton_kho_trung_bay + 5, 10) END,spl.quantity_f / 90))  end  PS_trung_bay,
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
    data_kiot_viet.master_dmhh dmhh ON spl.productcode = TRIM(dmhh.ma_hang)
		LEFT JOIN
	data_kiot_viet.master_leadtime_ncc ncc ON dmhh.ma_ncc = TRIM(ncc.ma_ncc)
        LEFT JOIN
    tf ON spl.productcode = tf.productCode
        AND spl.branchname = tf.TobranchName
        LEFT JOIN
    data_kiot_viet.master_leadtime_kho kho ON spl.branchname = kho.ma_kho_nhan
    WHERE spl.branchname NOT IN (N'1.HP.50 Lạch Tray (Q. Ngô Quyền)',N'1.HP.Kho Online',N'01.TỔNG KHO 2',
							     N'6.QN.60 Trần Nhân Tông (TP. Uông Bí)' -- bsung 12.09.2022
								)
    ;
    COMMIT;
ELSE
SET sql_mode = 'NO_ENGINE_SUBSTITUTION';
TRUNCATE TABLE data_kiot_viet.order_nchh_temp;
INSERT INTO data_kiot_viet.order_nchh_temp
WITH tf AS ( 
SELECT
    SUM(sendquantity) sendquantity, tobranchname, productcode
FROM
    data_kiot_viet.transfer
GROUP BY tobranchname , productcode
)
SELECT sysdate() event_date,
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
    spl.cost gia_von,
    spl.quantity_f,
    CAST(spl.onhand AS DECIMAL) onhand,
    spl.onhand_amount,
    IFNULL(tf.sendquantity, 0) sendquantity,
    dmhh.NV_CU,
    SUM(spl.quantity_f) OVER(PARTITION BY spl.productcode) quantity_f_all,
    spl.quantity_f / 90 DailySales_30,
    IFNULL(dmhh.Ngay_ton_kho_trung_bay, 7) Ngay_ton_kho_trung_bay,
    CEILING(data_kiot_viet.ps_trung_bay(dmhh.range_review, IFNULL(dmhh.Ngay_ton_kho_trung_bay,7),spl.quantity_f / 90)) PS_trung_bay,
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
    data_kiot_viet.master_dmhh dmhh ON spl.productcode = TRIM(dmhh.ma_hang)
		LEFT JOIN
	data_kiot_viet.master_leadtime_ncc ncc ON dmhh.ma_ncc = TRIM(ncc.ma_ncc)
        LEFT JOIN
    tf ON spl.productcode = tf.productCode
        AND spl.branchname = tf.TobranchName
        LEFT JOIN
    data_kiot_viet.master_leadtime_kho kho ON spl.branchname = kho.ma_kho_nhan
    WHERE spl.branchname NOT IN (N'1.HP.50 Lạch Tray (Q. Ngô Quyền)',N'1.HP.Kho Online',N'01.TỔNG KHO 2',
								 N'6.QN.60 Trần Nhân Tông (TP. Uông Bí)' -- bsung 12.09.2022)
                                 )
    ;
    COMMIT;
    END IF;
/*--------------------------------------------------!
02. BẢNG KẾT QUẢ
!--------------------------------------------------*/

SET sql_mode = 'NO_ENGINE_SUBSTITUTION';
TRUNCATE TABLE data_kiot_viet.order_nchh;
INSERT INTO data_kiot_viet.order_nchh
WITH tb AS (
SELECT 
    nchh.*,
    IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang) kho_giao_hang,
    CASE WHEN max.product_code IS NOT NULL THEN 1 ELSE
    ROUND(IFNULL(MIN_TK(nchh.range_review,
                            IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang),
                            nchh.PS_trung_bay,
                            nchh.DailySales_30,
                            nchh.tong_leadtime_kho,
                            nchh.Tan_suat_chia_hang_FQTO,
                            nchh.leadtime_giao_hang,
                            nchh.Tan_suat_dat_hang_FQPO),
                    0),
            0) END min,
    CEILING(CASE WHEN max.product_code IS NOT NULL THEN max.max ELSE
    CASE WHEN ROUND(MIN_TK(nchh.range_review,
                                    IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang),
                                    nchh.PS_trung_bay,
                                    nchh.DailySales_30,
                                    nchh.tong_leadtime_kho,
                                    nchh.Tan_suat_chia_hang_FQTO,
                                    nchh.leadtime_giao_hang,
                                    nchh.Tan_suat_dat_hang_FQPO),
                            0) <> 0
                        AND IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang) = N'01.TỔNG KHO 2'
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
                                    
                ELSE 0
            END END) max,
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
        THEN CEILING(CASE WHEN max.product_code IS NOT NULL THEN max.max ELSE
    CASE WHEN ROUND(MIN_TK(nchh.range_review,
                                    IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang),
                                    nchh.PS_trung_bay,
                                    nchh.DailySales_30,
                                    nchh.tong_leadtime_kho,
                                    nchh.Tan_suat_chia_hang_FQTO,
                                    nchh.leadtime_giao_hang,
                                    nchh.Tan_suat_dat_hang_FQPO),
                            0) <> 0
                        AND IFNULL(tf2.kho_giao_hang, tf1.kho_giao_hang) = N'01.TỔNG KHO 2'
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
                                    
                ELSE 0
            END END)
            - (nchh.onhand + nchh.sendquantity)
        ELSE 0
    END goi_y_dat_hang
FROM
    data_kiot_viet.order_nchh_temp nchh
        LEFT JOIN
    (SELECT * FROM data_kiot_viet.master_transfer WHERE (product_code IS NULL OR product_code = '')) tf1 ON nchh.branchName = tf1.chi_nhanh
        AND nchh.ma_ncc = tf1.ma_ncc
        LEFT JOIN
    (SELECT * FROM data_kiot_viet.master_transfer WHERE (product_code <> '' OR product_code IS NOT NULL)) tf2 ON nchh.productcode = tf2.product_code
		AND nchh.branchName = tf2.chi_nhanh
        AND nchh.ma_ncc = tf2.ma_ncc
        LEFT JOIN data_kiot_viet.master_max max
        ON nchh.productcode = max.product_code
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

/*--------------------------------------------------!
03. CẢNH BÁO LÔ DATE
!--------------------------------------------------*/

TRUNCATE TABLE data_kiot_viet.warning_lo_date;
INSERT INTO data_kiot_viet.warning_lo_date
WITH result AS (
SELECT 
    ivt.*, prd.code productcode,
    prd.fullname productname,
    STR_TO_DATE(RIGHT(ivt.fullnameVirgule,
                LENGTH(ivt.fullnameVirgule) - REGEXP_INSTR(ivt.fullnameVirgule, '-') - 1),
            '%d/%m/%Y') hsd,
    DATEDIFF(STR_TO_DATE(RIGHT(ivt.fullnameVirgule,
                        LENGTH(ivt.fullnameVirgule) - REGEXP_INSTR(ivt.fullnameVirgule, '-') - 1),
                    '%d/%m/%Y'),
            DATE(CURDATE())) diff_date,
    IFNULL(cs.quy_doi_ngay,30) quy_doi_ngay,
    CASE WHEN quy_doi_ngay IS NULL THEN 0 ELSE 1 END flag_date,
    CASE WHEN DATEDIFF(STR_TO_DATE(RIGHT(ivt.fullnameVirgule,
                        LENGTH(ivt.fullnameVirgule) - REGEXP_INSTR(ivt.fullnameVirgule, '-') - 1),
                    '%d/%m/%Y'),
            DATE(CURDATE())) <= IFNULL(cs.quy_doi_ngay,30) THEN 'Warning' ELSE NULL END flag_raise
FROM
    data_kiot_viet.inventory_exp ivt
        LEFT JOIN
    (SELECT 
        prd.id productid, cs.*
    FROM
        data_kiot_viet.master_cs_doidate cs
    INNER JOIN data_kiot_viet.product prd ON cs.ma_hang = prd.code) cs ON ivt.productid = cs.productId
        INNER JOIN
    data_kiot_viet.product prd ON ivt.productid = prd.id
    WHERE ivt.onhand <> 0
)
SELECT reportingdate, productcode, productname, br.branchName, onhand, batchname, hsd,diff_date, quy_doi_ngay,flag_date, flag_raise FROM result rs LEFT JOIN  
data_kiot_viet.branch br
ON rs.branchid = br.id
WHERE flag_raise = 'Warning';
COMMIT;
END;
END