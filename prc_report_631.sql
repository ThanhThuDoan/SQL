CREATE DEFINER=`root`@`%` PROCEDURE `prc_report_631`()
BEGIN

-- Doanh số năm hiện tại T

TRUNCATE TABLE data_kiot_viet.subtotal_curyear_temp631;
INSERT INTO data_kiot_viet.subtotal_curyear_temp631
SELECT ds_cy.productcode, SUM(ds_cy.quantity) quantity_cy, SUM(ds_cy.subtotal) subtotal_cy
FROM data_kiot_viet.daily_sales ds_cy
WHERE YEAR(ds_cy.createdDate) = YEAR(DATE_ADD(CURRENT_DATE, INTERVAL - 1 DAY))
GROUP BY ds_cy.productcode;

-- Lợi nhuận năm hiện tại T

TRUNCATE TABLE data_kiot_viet.profit_curyear_temp631;
INSERT INTO data_kiot_viet.profit_curyear_temp631
SELECT invt.productcode, invt.cost 
FROM data_kiot_viet.inventory invt
WHERE DATE(invt.reportingDate) = CURDATE()
AND invt.branchName = '01.TỔNG KHO 2';
COMMIT;

-- Doanh số năm T-1

TRUNCATE TABLE data_kiot_viet.subtotal_previousyear_temp631;
INSERT INTO data_kiot_viet.subtotal_previousyear_temp631
SELECT ds_py.productcode, SUM(ds_py.quantity) quantity_py, SUM(ds_py.subtotal) subtotal_py
FROM data_kiot_viet.daily_sales ds_py
WHERE YEAR(ds_py.createdDate) = YEAR(CURDATE()) - 1
GROUP BY ds_py.productcode;

-- Báo cáo 631:

TRUNCATE TABLE data_kiot_viet.auto_reporting_631;
SET sql_mode = 'NO_ENGINE_SUBSTITUTION';

INSERT INTO data_kiot_viet.auto_reporting_631
WITH rs_1 AS (
SELECT DISTINCT 
dmhh.ma_hang,
dmhh.ten_hang,
dmhh.brand,
dmhh.Loai_hh,
dmhh.mua_vu,
dmhh.Ma_nganh_hang,
dmhh.Ten_nganh_hang,
dmhh.Ma_nhom_nganh_hang,
dmhh.Ten_nhom_nganh_hang,
dmhh.Ma_nhom_sp,
dmhh.Ten_nhom_sp,
dmhh.Ma_Function,
dmhh.Ten_Function,
dmhh.Ngay_ton_kho_trung_bay,
dmhh.MOQ_PO,
dmhh.MOQ_TO,
dmhh.nv_cu,
IFNULL(py.quantity_py,0) quantity_py ,
IFNULL(py.subtotal_py,0) subtotal_py,
IFNULL(cy.quantity_cy,0) quantity_cy,
IFNULL(cy.subtotal_cy,0) subtotal_cy,
IFNULL(prf.cost,0) cost_cy,
ROUND(IFNULL(cy.subtotal_cy - prf.cost*cy.quantity_cy,0),0) profit_cy
FROM data_kiot_viet.master_dmhh dmhh
LEFT JOIN data_kiot_viet.subtotal_previousyear_temp631 py
ON dmhh.ma_hang = py.productCode
LEFT JOIN data_kiot_viet.subtotal_curyear_temp631 cy
ON dmhh.ma_hang = cy.productCode
LEFT JOIN data_kiot_viet.profit_curyear_temp631 prf
ON dmhh.ma_hang = prf.productCode
WHERE dmhh.ma_hang IS NOT NULL
AND dmhh.loai_hh NOT IN ('KM','TSCD','VTTH','QKM')
AND dmhh.ma_nganh_hang NOT IN ('HC','KM')
),
rs_2 AS 
(
SELECT 
    rs_1.*,
    CASE
        WHEN rs_1.quantity_cy = 0 THEN 'Bán=0'
        WHEN rs_1.profit_cy <= 0 THEN 'Âm GP'
        ELSE (rs_1.profit_cy * 0.5 / (SELECT 
                SUM(profit_cy)
            FROM
                rs_1) + rs_1.subtotal_cy * 0.4 / (SELECT 
                SUM(subtotal_cy)
            FROM
                rs_1) + rs_1.quantity_cy * 0.1 / (SELECT 
                SUM(quantity_cy)
            FROM
                rs_1))
    END bc541_cy
FROM
    rs_1
),
rs_3 AS (
SELECT rs_2.*, 
CASE WHEN rs_2.bc541_cy IN ('Bán=0','Âm GP') 
		THEN rs_2.bc541_cy
	ELSE SUM(CONVERT(rs_2.bc541_cy, DECIMAL(10,9))) OVER(ORDER BY CONVERT(rs_2.bc541_cy, DECIMAL(10,9)) DESC) END bc541_rt
FROM rs_2
),
rs_4 AS (
SELECT rs_3.*,
CASE WHEN rs_3.bc541_cy IN ('Bán=0','Âm GP') THEN rs_3.bc541_cy
	ELSE ROW_NUMBER() OVER(ORDER BY rs_3.bc541_rt) END rank_bc541
FROM rs_3
),
rs_5 AS (
SELECT rs_4.*,
(SELECT ROUND(COUNT(*)*0.97,0) FROM rs_4 WHERE rank_bc541 NOT IN ('Bán=0','Âm GP')) flag_rank,
CASE WHEN rank_bc541 NOT IN ('Bán=0','Âm GP') AND
CONVERT(rs_4.rank_bc541,DECIMAL(5,0)) <= (SELECT ROUND(COUNT(*)*0.97,0) FROM rs_4 WHERE rank_bc541 NOT IN ('Bán=0','Âm GP'))
THEN CONCAT('TOP ',(SELECT ROUND(COUNT(*)*0.97,0) FROM rs_4 WHERE rank_bc541 NOT IN ('Bán=0','Âm GP')))
ELSE CONCAT('TOP ',(SELECT ROUND(COUNT(*)*0.97,0) FROM rs_4 WHERE rank_bc541 NOT IN ('Bán=0','Âm GP')),'++') END top_bc541
FROM rs_4
),
rs_fnc AS
(
SELECT 
    ten_function,
    AVG(subtotal_cy) subtotal_cy_fc,
    AVG(profit_cy) profit_cy_fc
FROM
    rs_1
GROUP BY ten_function
),
rs_6 AS (
SELECT 
    rs_5.*,
    CASE
        WHEN rs_5.loai_hh IN ('QKM' , 'ORDER', 'TSCD', 'VTTH', 'CLO') THEN 'OUT RANGE'
        WHEN rs_5.loai_hh IN ('HHSX' , 'HM', 'KG') THEN N'CORE RANGE'
        ELSE CASE
            WHEN top_bc541 = CONCAT('TOP ', flag_rank, '++') THEN 'OUT RANGE'
            WHEN
                CONVERT( rs_5.rank_bc541 , DECIMAL (5 , 0 )) <= (SELECT 
                        ROUND(COUNT(*) * 0.20, 0)
                    FROM
                        rs_5
                    WHERE
                        rank_bc541 NOT IN ('Bán=0' , 'Âm GP'))
            THEN
                N'CORE RANGE'
            ELSE 'RANGE'
        END
    END range_review,
    IFNULL(rs_5.profit_cy / rs_5.subtotal_cy, 0) profit_cy_rate,
    rs_fnc.subtotal_cy_fc,
    rs_fnc.profit_cy_fc / rs_fnc.subtotal_cy_fc profit_cy_rate_fnc,
    CASE
        WHEN rs_5.bc541_cy = 'Âm GP' THEN 'Âm GP'
        WHEN
            (rs_5.bc541_cy = 'Bán=0'
                OR rs_5.loai_hh = 'CLO')
        THEN
            'P4'
        WHEN
            (rs_5.subtotal_cy >= rs_fnc.subtotal_cy_fc
                AND IFNULL(rs_5.profit_cy / rs_5.subtotal_cy, 0) >= IFNULL(rs_fnc.profit_cy_fc / rs_fnc.subtotal_cy_fc,
                    0))
        THEN
            'P1'
        WHEN
            (rs_5.subtotal_cy >= rs_fnc.subtotal_cy_fc
                AND IFNULL(rs_5.profit_cy / rs_5.subtotal_cy, 0) < IFNULL(rs_fnc.profit_cy_fc / rs_fnc.subtotal_cy_fc,
                    0))
        THEN
            'P2'
        WHEN
            (rs_5.subtotal_cy < rs_fnc.subtotal_cy_fc
                AND IFNULL(rs_5.profit_cy / rs_5.subtotal_cy, 0) >= IFNULL(rs_fnc.profit_cy_fc / rs_fnc.subtotal_cy_fc,
                    0))
        THEN
            'P3'
        ELSE 'P4'
    END rank_p
FROM
    rs_5
        LEFT JOIN
    rs_fnc ON rs_5.ten_function = rs_fnc.ten_function
)
SELECT sysdate() AS reportingDate,
rs_6.*,
CASE WHEN rs_6.rank_p = 'P2' 
	AND ROW_NUMBER() OVER(PARTITION BY rs_6.rank_p ORDER BY rs_6.quantity_cy DESC) <= 
						(SELECT COUNT(*) FROM rs_6 WHERE rank_p = 'P2')*0.2 THEN 'Nên sử dụng' ELSE NULL END recommend_prd
FROM rs_6;

COMMIT;

END