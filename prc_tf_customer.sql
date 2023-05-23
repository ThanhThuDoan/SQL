CREATE DEFINER=`root`@`%` PROCEDURE `prc_tf_customer`()
BEGIN
/*--------------------------------------------------!
Clean dữ liệu khách hàng
!--------------------------------------------------*/

SET sql_mode = 'NO_ENGINE_SUBSTITUTION';
TRUNCATE TABLE data_kiot_viet.customer_clean_data;

INSERT INTO data_kiot_viet.customer_clean_data
SELECT 
    cust.code customer_code,
    PROPER(cust.name) customer_name,
    DATE(cust.birthDate) birthDate,
    CASE
        WHEN DATE(cust.birthDate) IS NULL THEN NULL
        ELSE ROUND(DATEDIFF(CURDATE(), DATE(cust.birthDate)) / 30,
                0)
    END num_month_age,
    CASE
        WHEN cust.contactNumber = '' THEN NULL
        ELSE cust.contactNumber
    END contactNumber,
    br.branchName,
    CASE
        WHEN IFNULL(PROPER(cust.address), '') = '' THEN NULL
        ELSE PROPER(cust.address)
    END address,
    CASE
        WHEN IFNULL(cust.wardName, '') = '' THEN NULL
        ELSE cust.wardName
    END wardName,
    CASE
        WHEN IFNULL(cust.locationName, '') = '' THEN NULL
        ELSE MID(cust.locationName,
            REGEXP_INSTR(cust.locationName, '-') + 2,
            LENGTH(cust.locationName) - REGEXP_INSTR(cust.locationName, '-') - 1)
    END districtName,
    CASE
        WHEN IFNULL(cust.locationName, '') = '' THEN NULL
        ELSE LEFT(cust.locationName,
            REGEXP_INSTR(cust.locationName, '-') - 2)
    END cityName,
    CASE
        WHEN IFNULL(cust.locationName, '') = '' THEN NULL
        WHEN
            (REGEXP_INSTR(cust.locationName, '-') <> 0
                AND cust.locationName <> ''
                AND IFNULL(cust.wardName, '') <> '')
        THEN
            CONCAT(cust.wardName,
                    ' - ',
                    MID(cust.locationName,
                        REGEXP_INSTR(cust.locationName, '-') + 2,
                        LENGTH(cust.locationName) - REGEXP_INSTR(cust.locationName, '-') - 1),
                    ' - ',
                    LEFT(cust.locationName,
                        REGEXP_INSTR(cust.locationName, '-') - 2))
        WHEN
            (REGEXP_INSTR(cust.locationName, '-') <> 0
                AND cust.locationName <> ''
                AND IFNULL(cust.wardName, '') = '')
        THEN
            CONCAT(MID(cust.locationName,
                        REGEXP_INSTR(cust.locationName, '-') + 2,
                        LENGTH(cust.locationName) - REGEXP_INSTR(cust.locationName, '-') - 1),
                    ' - ',
                    LEFT(cust.locationName,
                        REGEXP_INSTR(cust.locationName, '-') - 2))
        ELSE NULL
    END locationName,
    cust.totalInvoiced,
    cust.totalRevenue,
    cust.totalPoint,
    cust.gender,
    CASE
        WHEN IFNULL(cust.email, '') = '' THEN NULL
        ELSE cust.email
    END email,
    DATE(cust.modifiedDate) modifiedDate,
    DATE(cust.createdDate) createdDate
FROM
    data_kiot_viet.customer cust
        LEFT JOIN
    data_kiot_viet.branch br ON cust.branchId = br.Id;

COMMIT;

END