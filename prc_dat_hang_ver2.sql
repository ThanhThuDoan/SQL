CREATE DEFINER=`root`@`%` PROCEDURE `prc_dat_hang_ver2`()
BEGIN

/*--------------------------------------------------!
01. Kết quả đặt hàng cho kho tổng
!--------------------------------------------------*/

TRUNCATE TABLE data_kiot_viet.dat_hang_v_ver2;
INSERT INTO data_kiot_viet.dat_hang_v_ver2
WITH oh_tk AS
(
SELECT SUM(onhand) onhand_tk, productcode
FROM
    data_kiot_viet.inventory
WHERE
    DATE(reportingdate) = CURDATE()
        AND branchid = '253685'
GROUP BY productcode
),
tf_to_tk AS
(
SELECT productCode, SUM(sendquantity) sendquantity_to_tk FROM data_kiot_viet.transfer WHERE tobranchname = N'01.TỔNG KHO 2'
GROUP BY productCode
),
tf_from_tk AS
(
SELECT productCode, SUM(sendquantity) sendquantity_from_tk FROM data_kiot_viet.transfer WHERE frombranchname = N'01.TỔNG KHO 2' AND status = '1'
GROUP BY productCode
),
cost_amt AS 
(
SELECT cost, productcode FROM  data_kiot_viet.inventory WHERE branchname = N'01.TỔNG KHO 2' AND DATE(reportingdate) = CURDATE()
),
dat_hang AS (
SELECT DISTINCT 
nchh.nv_cu,
nchh.productcode,
nchh.productname,
nchh.NCC_lendon,
nchh.Ma_NCC,
nchh.ten_ncc,
nchh.lich_dat_hang,
nchh.Leadtime_giao_hang,
nchh.Tan_suat_dat_hang_FQPO,
nchh.quantity_f_all,
oh_tk.onhand_tk,
IFNULL(ord_sup.quantity,0) order_quantity,
IFNULL(ord_sup.quantity,0) + IFNULL(tf_to_tk.sendquantity_to_tk,0) + oh_tk.onhand_tk - IFNULL(tf_from_tk.sendquantity_from_tk,0) Tk_du_kien,
cost_amt.cost gia_von,
IFNULL(tf_to_tk.sendquantity_to_tk,0) sendquantity_to_tk,
IFNULL(tf_from_tk.sendquantity_from_tk,0) sendquantity_from_tk,
nchh.MOQ_PO,
nchh.quantity_f_all/90*(nchh.Leadtime_giao_hang + nchh.Tan_suat_dat_hang_FQPO) Max_kho_tong,
SUM(nchh.so_dat_hang_chot) OVER(PARTITION BY nchh.productcode) tong_nhu_cau_dat_hang_CH
FROM data_kiot_viet.order_nchh_ver2 nchh LEFT JOIN oh_tk
ON nchh.productcode = oh_tk.productcode
LEFT JOIN tf_to_tk
ON nchh.productcode = tf_to_tk.productcode
LEFT JOIN tf_from_tk
ON nchh.productcode = tf_from_tk.productcode
LEFT JOIN cost_amt
ON nchh.productcode = cost_amt.productcode
LEFT JOIN (SELECT SUM(quantity) quantity, productid FROM data_kiot_viet.order_supplier GROUP BY productid) ord_sup
ON nchh.productid = ord_sup.productid
WHERE nchh.kho_giao_hang = N'01.TỔNG KHO 2'
)
SELECT productcode,
productname,
gia_von,
order_quantity,
Tk_du_kien,
Ma_NCC,
ten_ncc,
Leadtime_giao_hang,
Tan_suat_dat_hang_FQPO,
quantity_f_all,
onhand_tk,
sendquantity_to_tk,
sendquantity_from_tk,
MOQ_PO,
Max_kho_tong,
tong_nhu_cau_dat_hang_CH,
ROUND(CASE 
	WHEN dat_hang.max_kho_tong - (dat_hang.Tk_du_kien - dat_hang.tong_nhu_cau_dat_hang_CH) > 0 
		THEN  dat_hang.max_kho_tong - (dat_hang.Tk_du_kien - dat_hang.tong_nhu_cau_dat_hang_CH)
			ELSE 0 
END,0) slg_dat_goi_y,
CASE WHEN (ROUND(CASE 
	WHEN dat_hang.max_kho_tong - (dat_hang.Tk_du_kien - dat_hang.tong_nhu_cau_dat_hang_CH) > 0 
		THEN  dat_hang.max_kho_tong - (dat_hang.Tk_du_kien - dat_hang.tong_nhu_cau_dat_hang_CH)
			ELSE 0 
END,0) 
) >= 1/2*dat_hang.MOQ_PO AND
(
ROUND(CASE 
	WHEN dat_hang.max_kho_tong - (dat_hang.Tk_du_kien - dat_hang.tong_nhu_cau_dat_hang_CH) > 0 
		THEN  dat_hang.max_kho_tong - (dat_hang.Tk_du_kien - dat_hang.tong_nhu_cau_dat_hang_CH)
			ELSE 0 
END,0) 
) < dat_hang.MOQ_PO THEN dat_hang.MOQ_PO
ELSE
(
ROUND(CASE 
	WHEN dat_hang.max_kho_tong - (dat_hang.Tk_du_kien - dat_hang.tong_nhu_cau_dat_hang_CH) > 0 
		THEN  dat_hang.max_kho_tong - (dat_hang.Tk_du_kien - dat_hang.tong_nhu_cau_dat_hang_CH)
			ELSE 0 
END,0) 
) END slg_chot_dat,
CASE WHEN 
(
CASE WHEN (ROUND(CASE 
	WHEN dat_hang.max_kho_tong - (dat_hang.Tk_du_kien - dat_hang.tong_nhu_cau_dat_hang_CH) > 0 
		THEN  dat_hang.max_kho_tong - (dat_hang.Tk_du_kien - dat_hang.tong_nhu_cau_dat_hang_CH)
			ELSE 0 
END,0) 
) >= 1/2*dat_hang.MOQ_PO AND
(
ROUND(CASE
	WHEN dat_hang.max_kho_tong - (dat_hang.Tk_du_kien - dat_hang.tong_nhu_cau_dat_hang_CH) > 0 
		THEN  dat_hang.max_kho_tong - (dat_hang.Tk_du_kien - dat_hang.tong_nhu_cau_dat_hang_CH)
			ELSE 0 
END,0) 
) < dat_hang.MOQ_PO THEN dat_hang.MOQ_PO
ELSE
(
ROUND(CASE 
	WHEN dat_hang.max_kho_tong - (dat_hang.Tk_du_kien - dat_hang.tong_nhu_cau_dat_hang_CH) > 0 
		THEN  dat_hang.max_kho_tong - (dat_hang.Tk_du_kien - dat_hang.tong_nhu_cau_dat_hang_CH)
			ELSE 0 
END,0) 
) END
) > 0 THEN N'Tạo PO' ELSE NULL END tao_don_dat_hang,
lich_dat_hang, -- bsung 12.09.2022
nv_cu
FROM dat_hang;
COMMIT;

/*--------------------------------------------------!
02. Kết quả đặt hàng cho cửa hàng
!--------------------------------------------------*/
/*
CREATE VIEW data_kiot_viet.dat_hang_ch_v_ver2 AS 
SELECT 
    nchh.event_date,
    nchh.productcode,
    nchh.productname,
    nchh.branchname,
    nchh.NCC_lendon,
    nchh.Ma_NCC,
    nchh.Ten_NCC,
    ivt.cost gia_von,
    nchh.so_dat_hang_chot,
    nchh.nv_cu
FROM
    data_kiot_viet.order_nchh_ver2 nchh
        LEFT JOIN
    (SELECT * FROM data_kiot_viet.inventory_daily WHERE DATE(reportingDate) = CURDATE()) ivt ON nchh.productcode = ivt.productcode
        AND nchh.branchName = ivt.branchName
WHERE
    nchh.loai_don = N'Đơn đặt PO'
        AND nchh.branchName <> N'01.TỔNG KHO 2';
	
COMMIT;

*/

END