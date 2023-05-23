CREATE DEFINER=`root`@`%` PROCEDURE `prc_bi_pkd_1_hr`()
BEGIN

SET SQL_MODE = 'NO_ENGINE_SUBSTITUTION';
SET SQL_SAFE_UPDATES = 0;

/*--------------------------------------------------!
Báo cáo chất lượng nhân sự - tháng hiện tại
!--------------------------------------------------*/

TRUNCATE TABLE data_kiot_viet.sales_employee_rp_kd;

INSERT INTO data_kiot_viet.sales_employee_rp_kd
SELECT 
    br.m_rp,
    br.branchName,
    br.sales_code,
    hr.employee_name,
    SUM(br.slg_HD) slg_HD,
    SUM(br.slg_SKU) slg_SKU,
    SUM(br.quantity) quantity,
    SUM(br.subtotal_f) subtotal_f,
    hr.office_name
FROM
    data_kiot_viet.daily_sales_branch_all br
        LEFT JOIN
    (SELECT DISTINCT
        employee_code, employee_name, office_name
    FROM
        data_hrm.employee_turnover_rp_v) hr ON br.sales_code = hr.employee_code
WHERE
    m_rp IN (DATE_FORMAT(DATE(NOW() - INTERVAL 3 MONTH),'%Y-%m')
    ,DATE_FORMAT(DATE(NOW() - INTERVAL 2 MONTH),'%Y-%m')
    ,DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%Y-%m')
    )
         AND br.branchName NOT IN ('1.HP.Kho Online' , 'Hàng Ngoại Tỉnh (MKT)')
GROUP BY br.m_rp , br.branchName , br.sales_code , hr.employee_name , hr.office_name;
COMMIT;

/*--------------------------------------------------!
Báo cáo tra cứu thưởng nhân viên tháng T-1 và tháng T
!--------------------------------------------------*/

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
    DATE_FORMAT(a.createdDate,'%Y-%m') IN (DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%Y-%m')
    ,DATE_FORMAT(NOW(),'%Y-%m')
    )
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
    DATE_FORMAT(ds.createdDate,'%Y-%m') IN (DATE_FORMAT(DATE(NOW() - INTERVAL 1 MONTH),'%Y-%m')
    ,DATE_FORMAT(NOW(),'%Y-%m')
    )
AND ds.subtotal <> 0
AND ds.title <> 'QLCH'
ORDER BY createdDate, sales_code, branchname, nhom_hang;

COMMIT;

END