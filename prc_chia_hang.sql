CREATE DEFINER=`root`@`%` PROCEDURE `prc_chia_hang`()
BEGIN
/*--------------------------------------------------!
01. Get data ket qua chia hang, inventory exp
!--------------------------------------------------*/

TRUNCATE TABLE data_kiot_viet.chia_hang;
INSERT INTO data_kiot_viet.chia_hang
WITH list_ch1 AS (
SELECT N'01.TỔNG KHO 2' transfer_from_code,
nchh.range_review,
nchh.quantity_f,
nchh.productid,
nchh.productcode,
nchh.productname,
nchh.branchname,
nchh.Ma_nganh_hang,
nchh.Ten_nganh_hang,
nchh.Ten_Function,
nchh.Gia_von,
nchh.so_dat_hang_chot,
SUM(nchh.so_dat_hang_chot) OVER(PARTITION BY nchh.productcode) order_all,
nchh.nv_cu,
nchh.onhand,
oh_tk.onhand_tk,
CASE 
	WHEN nchh.onhand = 0 
		THEN MAX(nchh.quantity_f) OVER(PARTITION BY nchh.productcode) 
			ELSE quantity_f  
END rank_quantity
FROM data_kiot_viet.order_nchh nchh
LEFT JOIN (
SELECT SUM(onhand) onhand_tk, productcode
FROM (SELECT DISTINCT invt.* FROM
    data_kiot_viet.inventory invt
WHERE
    DATE_FORMAT(reportingdate, '%Y-%m-%d') = CURDATE()
        AND branchid = '253685') x -- Tổng kho
GROUP BY productcode) oh_tk
ON nchh.productcode = oh_tk.productcode
WHERE nchh.branchname <> N'01.TỔNG KHO 2'
AND nchh.Kho_giao_hang = N'01.TỔNG KHO 2'
),
list_ch2 AS (
SELECT list_ch1.*,
ROW_NUMBER() OVER(PARTITION BY productcode ORDER BY onhand ASC, rank_quantity DESC, quantity_f DESC, onhand ASC, so_dat_hang_chot DESC) row_num,
CASE WHEN order_all > onhand_tk AND onhand_tk <> 0 THEN
CASE WHEN ROW_NUMBER() OVER(PARTITION BY productcode ORDER BY onhand ASC, rank_quantity DESC, quantity_f DESC) = 1 AND onhand <= 0 THEN (onhand_tk - 1)
WHEN ROW_NUMBER() OVER(PARTITION BY productcode ORDER BY onhand ASC, rank_quantity DESC, quantity_f DESC) <> 1 AND onhand <= 0 THEN -1
ELSE (so_dat_hang_chot)*-1 END 
WHEN order_all > onhand_tk AND onhand_tk <= 0 THEN 0 ELSE so_dat_hang_chot END flag_1
FROM list_ch1
),
result_ch AS (
SELECT list_ch2.*,
SUM(flag_1) OVER(PARTITION BY productcode ORDER BY row_num) flag_2
FROM list_ch2)
SELECT result_ch.*,
CASE WHEN order_all > onhand_tk THEN
CASE WHEN  onhand_tk > 1 THEN
CASE WHEN  LAG(flag_2,1) OVER (PARTITION BY productcode ORDER BY row_num) IS NULL THEN 1
WHEN LAG(flag_2,1) OVER (PARTITION BY productcode ORDER BY row_num) <= 0 THEN 0
WHEN LAG(flag_2,1) OVER (PARTITION BY productcode ORDER BY row_num) >  0 AND flag_2 < 0 THEN LAG(flag_2,1) OVER (PARTITION BY productcode ORDER BY row_num)
ELSE ABS(flag_1) END 
WHEN onhand_tk = 1 AND row_num = 1 THEN 1 
WHEN onhand_tk = 0 THEN 0 ELSE 0 END
ELSE ABS(flag_1) END final_chia,
CASE WHEN (CASE WHEN order_all > onhand_tk THEN
CASE WHEN  onhand_tk > 1 THEN
CASE WHEN  LAG(flag_2,1) OVER (PARTITION BY productcode ORDER BY row_num) IS NULL THEN 1
WHEN LAG(flag_2,1) OVER (PARTITION BY productcode ORDER BY row_num) <= 0 THEN 0
WHEN LAG(flag_2,1) OVER (PARTITION BY productcode ORDER BY row_num) >  0 AND flag_2 < 0 THEN LAG(flag_2,1) OVER (PARTITION BY productcode ORDER BY row_num)
ELSE ABS(flag_1) END 
WHEN onhand_tk = 1 AND row_num = 1 THEN 1 
WHEN onhand_tk = 0 THEN 0 ELSE 0 END
ELSE ABS(flag_1) END
) > 0 THEN 'Đơn chia TO-SO' ELSE NULL END loai_don 
FROM result_ch;
COMMIT;

TRUNCATE TABLE data_kiot_viet.inventory_exp_tk;
INSERT INTO data_kiot_viet.inventory_exp_tk
SELECT * FROM data_kiot_viet.inventory_exp
WHERE branchID = '253685' AND onhand <> 0;
COMMIT;

TRUNCATE TABLE data_kiot_viet.chia_hang_f;
INSERT INTO data_kiot_viet.chia_hang_f
SELECT 
    ch.transfer_from_code,
    ch.range_review,
    ch.quantity_f,
    ch.productId,
    ch.productcode,
    ch.productname,
    ch.branchname,
    ch.ma_nganh_hang,
    ch.Ten_nganh_hang,
    ch.Ten_Function,
    ch.Gia_von,
    ch.so_dat_hang_chot,
    ch.order_all,
    ch.nv_cu,
    ch.onhand,
    ch.onhand_tk,
    ch.rank_quantity,
    ch.row_num,
    ch.flag_1,
    ch.flag_2,
    ch.final_chia,
    NULL batchname,
    NULL hsd,
    ch.loai_don
FROM
    data_kiot_viet.chia_hang ch WHERE
    NOT EXISTS (SELECT NULL FROM data_kiot_viet.inventory_exp b WHERE ch.productid = b.productId)
    AND ch.final_chia <> 0;
    COMMIT;
    
/*--------------------------------------------------!
02. Chia lo date
!--------------------------------------------------*/
    
BEGIN
DECLARE prd1 INTEGER DEFAULT 0;
DECLARE branch varchar(255) DEFAULT NULL;
DECLARE rn1 INTEGER  DEFAULT 0;
DECLARE slg_chia INTEGER DEFAULT 0;

DECLARE idb INTEGER DEFAULT 0;
DECLARE prd2 INTEGER DEFAULT 0;
DECLARE batn varchar(255) DEFAULT NULL;
DECLARE input_date DATE DEFAULT NULL;
DECLARE ohd varchar(255) DEFAULT NULL;
DECLARE hsd DATE DEFAULT NULL;
DECLARE rn2 INTEGER  DEFAULT 0;

DECLARE ch_done INTEGER DEFAULT 0;

/*--------------------------------------------------!
Declare cursor 1
!--------------------------------------------------*/

c1loop: LOOP

BEGIN

DECLARE c_1 CURSOR FOR
SELECT 
    productid, branchname, row_num, final_chia
FROM
    data_kiot_viet.chia_hang ch
WHERE final_chia <> 0 
AND EXISTS (SELECT NULL FROM data_kiot_viet.inventory_exp_tk exp WHERE ch.productid = exp.productId)
ORDER BY productid, row_num;

DECLARE CONTINUE HANDLER FOR NOT FOUND SET ch_done = 1;

IF ch_done = 1 
	THEN LEAVE c1loop;
END IF;

/*--------------------------------------------------!
Open cursor loop
!--------------------------------------------------*/
OPEN c_1;

FETCH NEXT FROM c_1 INTO prd1, branch, rn1, slg_chia;

SET SQL_SAFE_UPDATES = 0;

TRUNCATE TABLE data_kiot_viet.exp_temp;

INSERT INTO data_kiot_viet.exp_temp
SELECT id,
    productId,
    batchname,
    STR_TO_DATE(REPLACE(batchname,'/',''),'%d%m%Y') ngay_nhap,
    onhand,
    STR_TO_DATE(RIGHT(fullnameVirgule,
                LENGTH(fullnameVirgule) - REGEXP_INSTR(fullnameVirgule, '-') - 1),
            '%d/%m/%Y') hsd,
   ROW_NUMBER() OVER(PARTITION BY productid ORDER BY STR_TO_DATE(RIGHT(fullnameVirgule,
                LENGTH(fullnameVirgule) - REGEXP_INSTR(fullnameVirgule, '-') - 1),
            '%d/%m/%Y'), STR_TO_DATE(REPLACE(batchname,'/',''),'%d%m%Y')) rn
FROM
    data_kiot_viet.inventory_exp_tk WHERE onhand <> 0
AND productid = prd1;
    
COMMIT;

/*--------------------------------------------------!
Declare cursor 2
!--------------------------------------------------*/
BEGIN

DECLARE c_2 CURSOR FOR
SELECT * FROM  data_kiot_viet.exp_temp WHERE onhand <> 0 ORDER BY rn;

DECLARE CONTINUE HANDLER FOR NOT FOUND SET ch_done = 1;
IF ch_done = 1 THEN LEAVE c1loop;
END IF;

OPEN c_2;
FETCH NEXT FROM c_2 INTO idb,prd2, batn, input_date, ohd, hsd, rn2;

/*--------------------------------------------------!
Check case
!--------------------------------------------------*/

IF slg_chia = ohd THEN
INSERT INTO data_kiot_viet.chia_hang_f
SELECT 
    ch.transfer_from_code,
    ch.range_review,
    ch.quantity_f,
    ch.productId,
    ch.productcode,
    ch.productname,
    ch.branchname,
    ch.ma_nganh_hang,
    ch.Ten_nganh_hang,
    ch.Ten_Function,
    ch.Gia_von,
    ch.so_dat_hang_chot,
    ch.order_all,
    ch.nv_cu,
    ch.onhand,
    exp_temp.onhand onhand_tk,
    ch.rank_quantity,
    ch.row_num,
    ch.flag_1,
    ch.flag_2,
    ch.final_chia,
    exp_temp.batchname,
    exp_temp.hsd,
    ch.loai_don
FROM
    data_kiot_viet.chia_hang ch
        INNER JOIN
    data_kiot_viet.exp_temp ON ch.productid = exp_temp.productid
    WHERE ch.productid = prd1
    AND ch.branchname = branch
    AND exp_temp.batchname = batn
    AND exp_temp.id = idb;
COMMIT;
UPDATE data_kiot_viet.inventory_exp_tk
SET onhand = 0
WHERE id = idb
AND batchname = batn;
COMMIT;
UPDATE data_kiot_viet.chia_hang
SET final_chia = 0
WHERE productid = prd1
AND branchname = branch;
COMMIT;
ELSEIF slg_chia < ohd THEN
INSERT INTO data_kiot_viet.chia_hang_f
SELECT 
    ch.transfer_from_code,
    ch.range_review,
    ch.quantity_f,
    ch.productId,
    ch.productcode,
    ch.productname,
    ch.branchname,
    ch.ma_nganh_hang,
    ch.Ten_nganh_hang,
    ch.Ten_Function,
    ch.Gia_von,
    ch.so_dat_hang_chot,
    ch.order_all,
    ch.nv_cu,
    ch.onhand,
    exp_temp.onhand onhand_tk,
    ch.rank_quantity,
    ch.row_num,
    ch.flag_1,
    ch.flag_2,
    ch.final_chia,
    exp_temp.batchname,
    exp_temp.hsd,
    ch.loai_don
FROM
    data_kiot_viet.chia_hang ch
        INNER JOIN
    data_kiot_viet.exp_temp ON ch.productid = exp_temp.productid
    WHERE ch.productid = prd1
    AND ch.branchname = branch
    AND exp_temp.batchname = batn
    AND exp_temp.id = idb;
COMMIT;
UPDATE data_kiot_viet.inventory_exp_tk
SET onhand = ohd - slg_chia
WHERE id = idb
AND batchname = batn;
COMMIT;
UPDATE data_kiot_viet.chia_hang
SET final_chia = 0
WHERE productid = prd1
AND branchname = branch;
COMMIT;
ELSEIF slg_chia > ohd THEN
INSERT INTO data_kiot_viet.chia_hang_f
SELECT 
    ch.transfer_from_code,
    ch.range_review,
    ch.quantity_f,
    ch.productId,
    ch.productcode,
    ch.productname,
    ch.branchname,
    ch.ma_nganh_hang,
    ch.Ten_nganh_hang,
    ch.Ten_Function,
    ch.Gia_von,
    ch.so_dat_hang_chot,
    ch.order_all,
    ch.nv_cu,
    ch.onhand,
    exp_temp.onhand onhand_tk,
    ch.rank_quantity,
    ch.row_num,
    ch.flag_1,
    ch.flag_2,
    exp_temp.onhand final_chia,
    exp_temp.batchname,
    exp_temp.hsd,
    ch.loai_don
FROM
    data_kiot_viet.chia_hang ch
        INNER JOIN
    data_kiot_viet.exp_temp ON ch.productid = exp_temp.productid
    WHERE ch.productid = prd1
    AND ch.branchname = branch
    AND exp_temp.batchname = batn
    AND exp_temp.id = idb;
COMMIT;
UPDATE data_kiot_viet.chia_hang
SET final_chia = slg_chia - ohd
WHERE productid = prd1
AND branchname = branch;
COMMIT;
UPDATE data_kiot_viet.inventory_exp_tk
SET onhand = 0
WHERE id = idb;
COMMIT;
END IF;
CLOSE c_2;
END;
CLOSE c_1;
END;
END LOOP c1loop;
END;
END