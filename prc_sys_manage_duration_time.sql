CREATE DEFINER=`root`@`%` PROCEDURE `prc_sys_manage_duration_time`()
BEGIN
SET SQL_MODE = 'NO_ENGINE_SUBSTITUTION';
SET SQL_SAFE_UPDATES = 0;
/*--------------------------------------------------!
0. Check data source from kiotviet
!--------------------------------------------------*/

INSERT INTO data_kiot_viet.sys_load_manage_duration_time
WITH rs AS (
SELECT sysdate() dayid,'kiotviet' datasource, 'customer' major_type, 'data_kiot_viet' table_schema, 'customer' table_name, (SELECT DATE(max(createdDate)) FROM data_kiot_viet.customer) last_update_date
UNION ALL
SELECT sysdate() dayid,'kiotviet' datasource, 'inventory' major_type, 'data_kiot_viet' table_schema, 'inventory' table_name, (SELECT  MAX(reportingdate) FROM data_kiot_viet.inventory) last_update_date
UNION ALL
SELECT sysdate() dayid,'kiotviet' datasource, 'inventory' major_type, 'data_kiot_viet' table_schema, 'inventory_daily' table_name, (SELECT  MAX(reportingdate) FROM data_kiot_viet.inventory_daily) last_update_date
UNION ALL
#SELECT sysdate() dayid,'kiotviet' datasource, 'inventory' major_type, 'data_kiot_viet' table_schema, 'inventory_exp' table_name, (SELECT  MAX(reportingdate) FROM data_kiot_viet.inventory_exp) last_update_date
#UNION ALL
SELECT sysdate() dayid,'kiotviet' datasource, 'product' major_type, 'data_kiot_viet' table_schema, 'product' table_name, (SELECT  MAX(reportingdate) FROM data_kiot_viet.product) last_update_date
UNION ALL
SELECT sysdate() dayid,'kiotviet' datasource, 'order' major_type, 'data_kiot_viet' table_schema, 'order_supplier' table_name, (SELECT max(reportingDate) FROM data_kiot_viet.order_supplier) last_update_date
UNION ALL
SELECT sysdate() dayid,'kiotviet' datasource, 'transfer' major_type, 'data_kiot_viet' table_schema, 'transfer' table_name, (SELECT max(reportingDate) FROM data_kiot_viet.transfer) last_update_date
UNION ALL
SELECT sysdate() dayid,'kiotviet' datasource, 'invoices' major_type, 'data_kiot_viet' table_schema, 'invoices' table_name, (SELECT DATE(MAX(createdDate)) FROM data_kiot_viet.invoices) last_update_date
UNION ALL
SELECT sysdate() dayid,'kiotviet' datasource, 'invoices' major_type, 'data_kiot_viet' table_schema, 'invoice_details' table_name, (SELECT DATE(MAX(createdDate)) FROM data_kiot_viet.invoice_details) last_update_date
UNION ALL
SELECT sysdate() dayid,'kiotviet' datasource, 'returns' major_type, 'data_kiot_viet' table_schema, 'returns' table_name, (SELECT DATE(MAX(createdDate)) FROM data_kiot_viet.returns) last_update_date
UNION ALL
SELECT sysdate() dayid,'kiotviet' datasource, 'returns' major_type, 'data_kiot_viet' table_schema, 'returns_details' table_name, (SELECT DATE(MAX(createdDate)) FROM data_kiot_viet.returns_details) last_update_date
UNION ALL
SELECT sysdate() dayid,'kiotviet' datasource, 'invoices' major_type, 'data_kiot_viet' table_schema, 'invoices_2022' table_name, (SELECT DATE(MAX(createdDate)) FROM data_kiot_viet.invoices_2022) last_update_date
UNION ALL
SELECT sysdate() dayid,'kiotviet' datasource, 'invoices' major_type, 'data_kiot_viet' table_schema, 'invoice_details_2022' table_name, (SELECT DATE(MAX(createdDate)) FROM data_kiot_viet.invoice_details_2022) last_update_date
UNION ALL
SELECT sysdate() dayid,'kiotviet' datasource, 'returns' major_type, 'data_kiot_viet' table_schema, 'returns_2022' table_name, (SELECT DATE(MAX(createdDate)) FROM data_kiot_viet.returns_2022) last_update_date
UNION ALL
SELECT sysdate() dayid,'kiotviet' datasource, 'returns' major_type, 'data_kiot_viet' table_schema, 'returns_details_2022' table_name, (SELECT DATE(MAX(createdDate)) FROM data_kiot_viet.returns_details_2022) last_update_date
UNION ALL
SELECT sysdate() dayid,'kiotviet' datasource, 'payment' major_type, 'data_kiot_viet' table_schema, 'payment_details' table_name, (SELECT DATE(max(transDate)) FROM data_kiot_viet.payment_details) last_update_date
)
SELECT rs.*,
CASE WHEN major_type IN ('returns','invoices','customer','payment') AND DATE(last_update_date) = DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN 'Pass'
	#WHEN table_name = 'inventory_exp' AND DATE(last_update_date) = DATE_ADD(CURDATE(), INTERVAL -1 DAY) THEN 'Pass'
    WHEN (major_type NOT IN ('returns','invoices','customer','payment') #OR table_name <> 'inventory_exp'
    ) AND DATE(last_update_date) = CURDATE() THEN 'Pass'
    ELSE 'Fail' END status
FROM rs;

COMMIT;

/*--------------------------------------------------!
1. Call procedure data preparation:
!--------------------------------------------------*/

-- Daily sales product all:

INSERT INTO data_kiot_viet.sys_tf_manage_duration_time (dayid,schema_name,procedure_name,table_name,last_update_date,start_time,end_time,duration_time,status)
VALUES
(CURDATE(),'data_kiot_viet','prc_tf_daily_sales_product_all','daily_sales',NULL,NOW(),NULL,NULL,NULL);
COMMIT;

CALL data_kiot_viet.prc_tf_daily_sales_product_all(DATE_ADD(CURDATE(), INTERVAL -7 DAY), DATE_ADD(CURDATE(), INTERVAL - 1 DAY));

UPDATE data_kiot_viet.sys_tf_manage_duration_time 
SET 
    last_update_date = SYSDATE(),
    end_time = NOW(),
    duration_time = TIME_TO_SEC(TIMEDIFF(NOW(), start_time)),
    status = CASE
        WHEN TIME_TO_SEC(TIMEDIFF(NOW(),start_time)) > 0 THEN 'Pass'
        ELSE 'Fail'
    END
WHERE
    dayid = CURDATE()
        AND schema_name = 'data_kiot_viet'
        AND procedure_name = 'prc_tf_daily_sales_product_all'
        AND table_name = 'daily_sales'
        AND end_time IS NULL;
COMMIT;

-- Daily sales branch all:

INSERT INTO data_kiot_viet.sys_tf_manage_duration_time (dayid,schema_name,procedure_name,table_name,last_update_date,start_time,end_time,duration_time,status)
VALUES
(CURDATE(),'data_kiot_viet','prc_tf_daily_sales_branch_all','daily_sales_branch_all',NULL,NOW(),NULL,NULL,NULL);
COMMIT;

CALL data_kiot_viet.prc_tf_daily_sales_branch_all(DATE_ADD(CURDATE(), INTERVAL -32 DAY), DATE_ADD(CURDATE(), INTERVAL - 1 DAY));

UPDATE data_kiot_viet.sys_tf_manage_duration_time
SET 
    last_update_date = SYSDATE(),
    end_time = NOW(),
    duration_time = TIME_TO_SEC(TIMEDIFF(NOW(), start_time)),
    status = CASE
        WHEN TIME_TO_SEC(TIMEDIFF(NOW(),start_time)) > 0 THEN 'Pass'
        ELSE 'Fail'
    END
WHERE
    dayid = CURDATE()
        AND schema_name = 'data_kiot_viet'
        AND procedure_name = 'prc_tf_daily_sales_branch_all'
        AND table_name = 'daily_sales_branch_all'
        AND end_time IS NULL;
COMMIT;

-- Customer

INSERT INTO data_kiot_viet.sys_tf_manage_duration_time (dayid,schema_name,procedure_name,table_name,last_update_date,start_time,end_time,duration_time,status)
VALUES
(CURDATE(),'data_kiot_viet','prc_tf_customer','customer_clean_data',NULL,NOW(),NULL,NULL,NULL);
COMMIT;

CALL data_kiot_viet.prc_tf_customer;

UPDATE data_kiot_viet.sys_tf_manage_duration_time 
SET 
    last_update_date = SYSDATE(),
    end_time = NOW(),
    duration_time = TIME_TO_SEC(TIMEDIFF(NOW(), start_time)),
    status = CASE
        WHEN TIME_TO_SEC(TIMEDIFF(NOW(),start_time)) > 0 THEN 'Pass'
        ELSE 'Fail'
    END
WHERE
    dayid = CURDATE()
        AND schema_name = 'data_kiot_viet'
        AND procedure_name = 'prc_tf_customer'
        AND table_name = 'customer_clean_data'
        AND end_time IS NULL;
COMMIT;

/*--------------------------------------------------!
2. Call procedure BI Report - Phòng Chuỗi Cung ứng
!--------------------------------------------------*/

-- Số bán 90 ngày + Báo cáo 631 + OOS + Master DMHH ver2

INSERT INTO data_kiot_viet.sys_bi_manage_duration_time (dayid,schema_name,procedure_name,table_name,last_update_date,start_time,end_time,duration_time,status)
VALUES
(CURDATE(),'data_kiot_viet','prc_bi_pccu_1','Supply_90days, subtotal_curyear_temp631, profit_curyear_temp631, subtotal_previousyear_temp631, auto_reporting_631, daily_sales_90days_0, inventory_90days_onhand0_ver02, master_dmhh_ver2',NULL,NOW(),NULL,NULL,NULL);
COMMIT;

CALL data_kiot_viet.prc_bi_pccu_1;

UPDATE data_kiot_viet.sys_bi_manage_duration_time 
SET 
    last_update_date = SYSDATE(),
    end_time = NOW(),
    duration_time = TIME_TO_SEC(TIMEDIFF(NOW(), start_time)),
    status = CASE
        WHEN TIME_TO_SEC(TIMEDIFF(NOW(),start_time)) > 0 THEN 'Pass'
        ELSE 'Fail'
    END
WHERE
    dayid = CURDATE()
        AND schema_name = 'data_kiot_viet'
        AND procedure_name = 'prc_bi_pccu_1'
        AND end_time IS NULL;
COMMIT;

-- Nhu cầu hàng hoá

INSERT INTO data_kiot_viet.sys_bi_manage_duration_time (dayid,schema_name,procedure_name,table_name,last_update_date,start_time,end_time,duration_time,status)
VALUES
(CURDATE(),'data_kiot_viet','prc_bi_pccu_2_nchh','order_nchh_ver2',NULL,NOW(),NULL,NULL,NULL);
COMMIT;

CALL data_kiot_viet.prc_bi_pccu_2_nchh;

UPDATE data_kiot_viet.sys_bi_manage_duration_time 
SET 
    last_update_date = SYSDATE(),
    end_time = NOW(),
    duration_time = TIME_TO_SEC(TIMEDIFF(NOW(), start_time)),
    status = CASE
        WHEN TIME_TO_SEC(TIMEDIFF(NOW(),start_time)) > 0 THEN 'Pass'
        ELSE 'Fail'
    END
WHERE
    dayid = CURDATE()
        AND schema_name = 'data_kiot_viet'
        AND procedure_name = 'prc_bi_pccu_2_nchh'
        AND table_name = 'order_nchh_ver2'
        AND end_time IS NULL;
        
COMMIT;

-- Chia hàng

INSERT INTO data_kiot_viet.sys_bi_manage_duration_time (dayid,schema_name,procedure_name,table_name,last_update_date,start_time,end_time,duration_time,status)
VALUES
(CURDATE(),'data_kiot_viet','prc_bi_pccu_2_chia_hang','chia_hang_f_ver2',NULL,NOW(),NULL,NULL,NULL);
COMMIT;

CALL data_kiot_viet.prc_bi_pccu_2_chia_hang;

UPDATE data_kiot_viet.sys_bi_manage_duration_time 
SET 
    last_update_date = SYSDATE(),
    end_time = NOW(),
    duration_time = TIME_TO_SEC(TIMEDIFF(NOW(), start_time)),
    status = CASE
        WHEN TIME_TO_SEC(TIMEDIFF(NOW(),start_time)) > 0 THEN 'Pass'
        ELSE 'Fail'
    END
WHERE
    dayid = CURDATE()
        AND schema_name = 'data_kiot_viet'
        AND procedure_name = 'prc_bi_pccu_2_chia_hang'
        AND table_name = 'chia_hang_f_ver2'
        AND end_time IS NULL;
        
COMMIT;

-- Đặt hàng

INSERT INTO data_kiot_viet.sys_bi_manage_duration_time (dayid,schema_name,procedure_name,table_name,last_update_date,start_time,end_time,duration_time,status)
VALUES
(CURDATE(),'data_kiot_viet','prc_bi_pccu_2_dat_hang','dat_hang_ver2',NULL,NOW(),NULL,NULL,NULL);
COMMIT;

CALL data_kiot_viet.prc_bi_pccu_2_dat_hang;

UPDATE data_kiot_viet.sys_bi_manage_duration_time 
SET 
    last_update_date = SYSDATE(),
    end_time = NOW(),
    duration_time = TIME_TO_SEC(TIMEDIFF(NOW(), start_time)),
    status = CASE
        WHEN TIME_TO_SEC(TIMEDIFF(NOW(),start_time)) > 0 THEN 'Pass'
        ELSE 'Fail'
    END
WHERE
    dayid = CURDATE()
        AND schema_name = 'data_kiot_viet'
        AND procedure_name = 'prc_bi_pccu_2_dat_hang'
        AND table_name = 'dat_hang_ver2'
        AND end_time IS NULL;
        
COMMIT;

-- Tính KPI

INSERT INTO data_kiot_viet.sys_bi_manage_duration_time (dayid,schema_name,procedure_name,table_name,last_update_date,start_time,end_time,duration_time,status)
VALUES
(CURDATE(),'data_kiot_viet','prc_bi_pccu_2_kpi','kpi_thumua_report, kpi_cungung_report',NULL,NOW(),NULL,NULL,NULL);
COMMIT;

CALL data_kiot_viet.prc_bi_pccu_2_kpi;

UPDATE data_kiot_viet.sys_bi_manage_duration_time 
SET 
    last_update_date = SYSDATE(),
    end_time = NOW(),
    duration_time = TIME_TO_SEC(TIMEDIFF(NOW(), start_time)),
    status = CASE
        WHEN TIME_TO_SEC(TIMEDIFF(NOW(),start_time)) > 0 THEN 'Pass'
        ELSE 'Fail'
    END
WHERE
    dayid = CURDATE()
        AND schema_name = 'data_kiot_viet'
        AND procedure_name = 'prc_bi_pccu_2_kpi'
        AND table_name = 'kpi_thumua_report, kpi_cungung_report'
        AND end_time IS NULL;
        
COMMIT;

/*--------------------------------------------------!
3. Call procedure BI Report - Phòng Kinh doanh
!--------------------------------------------------*/

-- Mục tiêu thực đạt

INSERT INTO data_kiot_viet.sys_bi_manage_duration_time (dayid,schema_name,procedure_name,table_name,last_update_date,start_time,end_time,duration_time,status)
VALUES
(CURDATE(),'data_kiot_viet','prc_bi_pkd_1_target_actual','master_sales_target_daily, daily_sales_asm_rp_v',NULL,NOW(),NULL,NULL,NULL);
COMMIT;

CALL data_kiot_viet.prc_bi_pkd_1_target_actual;
CALL data_kiot_viet.prc_bi_pkd_1_target_actual_prv;

UPDATE data_kiot_viet.sys_bi_manage_duration_time 
SET 
    last_update_date = SYSDATE(),
    end_time = NOW(),
    duration_time = TIME_TO_SEC(TIMEDIFF(NOW(), start_time)),
    status = CASE
        WHEN TIME_TO_SEC(TIMEDIFF(NOW(),start_time)) > 0 THEN 'Pass'
        ELSE 'Fail'
    END
WHERE
    dayid = CURDATE()
        AND schema_name = 'data_kiot_viet'
        AND procedure_name = 'prc_bi_pkd_1_target_actual'
        AND table_name = 'master_sales_target_daily, daily_sales_asm_rp_v'
        AND end_time IS NULL;
        
COMMIT;

-- Doanh số ngành hàng

INSERT INTO data_kiot_viet.sys_bi_manage_duration_time (dayid,schema_name,procedure_name,table_name,last_update_date,start_time,end_time,duration_time,status)
VALUES
(CURDATE(),'data_kiot_viet','prc_bi_pkd_1_sku_revenue','daily_sales_product_kd, pkd_sosanh_cungky_nh, pkd_sosanh_cungky_nh_mtd',NULL,NOW(),NULL,NULL,NULL);
COMMIT;

CALL data_kiot_viet.prc_bi_pkd_1_sku_revenue;

UPDATE data_kiot_viet.sys_bi_manage_duration_time 
SET 
    last_update_date = SYSDATE(),
    end_time = NOW(),
    duration_time = TIME_TO_SEC(TIMEDIFF(NOW(), start_time)),
    status = CASE
        WHEN TIME_TO_SEC(TIMEDIFF(NOW(),start_time)) > 0 THEN 'Pass'
        ELSE 'Fail'
    END
WHERE
    dayid = CURDATE()
        AND schema_name = 'data_kiot_viet'
        AND procedure_name = 'prc_bi_pkd_1_sku_revenue'
        AND table_name = 'daily_sales_product_kd, pkd_sosanh_cungky_nh, pkd_sosanh_cungky_nh_mtd'
        AND end_time IS NULL;
        
COMMIT;

-- Phân loại khách hàng

INSERT INTO data_kiot_viet.sys_bi_manage_duration_time (dayid,schema_name,procedure_name,table_name,last_update_date,start_time,end_time,duration_time,status)
VALUES
(CURDATE(),'data_kiot_viet','prc_bi_pkd_1_customer','customer_by_branch',NULL,NOW(),NULL,NULL,NULL);
COMMIT;

CALL data_kiot_viet.prc_bi_pkd_1_customer;

UPDATE data_kiot_viet.sys_bi_manage_duration_time 
SET 
    last_update_date = SYSDATE(),
    end_time = NOW(),
    duration_time = TIME_TO_SEC(TIMEDIFF(NOW(), start_time)),
    status = CASE
        WHEN TIME_TO_SEC(TIMEDIFF(NOW(),start_time)) > 0 THEN 'Pass'
        ELSE 'Fail'
    END
WHERE
    dayid = CURDATE()
        AND schema_name = 'data_kiot_viet'
        AND procedure_name = 'prc_bi_pkd_1_customer'
        AND table_name = 'customer_by_branch'
        AND end_time IS NULL;
        
COMMIT;

-- Chất lượng nhân sự

INSERT INTO data_kiot_viet.sys_bi_manage_duration_time (dayid,schema_name,procedure_name,table_name,last_update_date,start_time,end_time,duration_time,status)
VALUES
(CURDATE(),'data_kiot_viet','prc_bi_pkd_1_hr','sales_employee_rp_kd, tra_cuu_sales_incentive',NULL,NOW(),NULL,NULL,NULL);
COMMIT;

CALL data_kiot_viet.prc_bi_pkd_1_hr;

UPDATE data_kiot_viet.sys_bi_manage_duration_time 
SET 
    last_update_date = SYSDATE(),
    end_time = NOW(),
    duration_time = TIME_TO_SEC(TIMEDIFF(NOW(), start_time)),
    status = CASE
        WHEN TIME_TO_SEC(TIMEDIFF(NOW(),start_time)) > 0 THEN 'Pass'
        ELSE 'Fail'
    END
WHERE
    dayid = CURDATE()
        AND schema_name = 'data_kiot_viet'
        AND procedure_name = 'prc_bi_pkd_1_hr'
        AND table_name = 'sales_employee_rp_kd, tra_cuu_sales_incentive'
        AND end_time IS NULL;
        
COMMIT;

/*--------------------------------------------------!
4. Call procedure BI Report - Phòng Marketing
!--------------------------------------------------*/

-- Daily Sales + Số bán 30 ngày

INSERT INTO data_kiot_viet.sys_bi_manage_duration_time (dayid,schema_name,procedure_name,table_name,last_update_date,start_time,end_time,duration_time,status)
VALUES
(CURDATE(),'data_kiot_viet','prc_bi_mkt_1','Supply_30days, rp_30days_mkt, daily_sales_marketing, customer_by_region',NULL,NOW(),NULL,NULL,NULL);
COMMIT;

CALL data_kiot_viet.prc_bi_mkt_1;

UPDATE data_kiot_viet.sys_bi_manage_duration_time 
SET 
    last_update_date = SYSDATE(),
    end_time = NOW(),
    duration_time = TIME_TO_SEC(TIMEDIFF(NOW(), start_time)),
    status = CASE
        WHEN TIME_TO_SEC(TIMEDIFF(NOW(),start_time)) > 0 THEN 'Pass'
        ELSE 'Fail'
    END
WHERE
    dayid = CURDATE()
        AND schema_name = 'data_kiot_viet'
        AND procedure_name = 'prc_bi_mkt_1'
        AND table_name = 'Supply_30days, rp_30days_mkt, daily_sales_marketing, customer_by_region'
        AND end_time IS NULL;
        
COMMIT;

-- Chi phí/Doanh thu dự kiến - thực đạt

INSERT INTO data_kiot_viet.sys_bi_manage_duration_time (dayid,schema_name,procedure_name,table_name,last_update_date,start_time,end_time,duration_time,status)
VALUES
(CURDATE(),'data_kiot_viet','prc_bi_mkt_cost_revenue','master_mkt_target_daily,bi_mkt_cost_revenue_v',NULL,NOW(),NULL,NULL,NULL);
COMMIT;

CALL data_kiot_viet.prc_bi_mkt_1;

UPDATE data_kiot_viet.sys_bi_manage_duration_time 
SET 
    last_update_date = SYSDATE(),
    end_time = NOW(),
    duration_time = TIME_TO_SEC(TIMEDIFF(NOW(), start_time)),
    status = CASE
        WHEN TIME_TO_SEC(TIMEDIFF(NOW(),start_time)) > 0 THEN 'Pass'
        ELSE 'Fail'
    END
WHERE
    dayid = CURDATE()
        AND schema_name = 'data_kiot_viet'
        AND procedure_name = 'prc_bi_mkt_cost_revenue'
        AND table_name = 'master_mkt_target_daily,bi_mkt_cost_revenue_v'
        AND end_time IS NULL;
        
COMMIT;

/*--------------------------------------------------!
5. Call procedure BI Report - BOD
!--------------------------------------------------*/

CALL data_kiot_viet.prc_bod_report;
CALL data_kiot_viet.prc_bod_rp_daily;
COMMIT;

END