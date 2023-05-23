CREATE DEFINER=`root`@`%` PROCEDURE `customer_old_level`()
BEGIN
TRUNCATE TABLE customer_old_level; 

COMMIT;

INSERT INTO customer_old_level 
(customerCode, name, contactNumber, num_invoices, total_spent, aov, level)
SELECT a.customerCode, b.name, b.contactNumber,  
COUNT(a.code) as num_invoices,  
SUM(a.total) as total_spent,
ROUND((SUM(a.total))/(COUNT(a.code)), 0) as aov,  
CASE  
WHEN COUNT(a.code) >= 2 AND ROUND((SUM(a.total))/(COUNT(a.code)), 0) >= 1000000 THEN 'Nhóm C1: KH VIP'  
WHEN COUNT(a.code) < 2 AND ROUND((SUM(a.total))/(COUNT(a.code)), 0) >= 1000000 THEN 'Nhóm C2: KH Pre-VIP'  
WHEN COUNT(a.code) >= 2 AND 200000 <= ROUND((SUM(a.total))/(COUNT(a.code)), 0) < 1000000 THEN 'Nhóm C3: KH Trung thành' 
WHEN COUNT(a.code) >= 2 AND ROUND((SUM(a.total))/(COUNT(a.code)), 0) < 200000 THEN 'Nhóm C3: KH Săn sale'  
WHEN COUNT(a.code) < 2 AND 200000 <= ROUND((SUM(a.total))/(COUNT(a.code)), 0) < 1000000 THEN 'Nhóm C4: Tiềm năng'
WHEN COUNT(a.code) < 2 AND 0 < ROUND((SUM(a.total))/(COUNT(a.code)), 0) < 200000 THEN 'Nhóm C4: Tiềm năng'
ELSE 'Không xác định'
END as level
FROM data_kiot_viet.invoices a  
JOIN data_kiot_viet.customer b ON a.customerCode = b.code
WHERE a.createdDate >= DATE_SUB(NOW(), INTERVAL 2 MONTH) AND a.createdDate < DATE_SUB(NOW(), INTERVAL 1 MONTH)
AND NOT EXISTS (
  SELECT 1
  FROM data_kiot_viet.invoices i
  WHERE i.customerCode = a.customerCode AND i.createdDate >= DATE_SUB(NOW(), INTERVAL 1 MONTH) AND i.createdDate < NOW()
)
AND LENGTH(b.contactNumber) <= 50
GROUP BY a.customerCode, b.name, b.contactNumber;
END