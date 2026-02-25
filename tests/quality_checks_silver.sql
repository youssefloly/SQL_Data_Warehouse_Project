/*
======================================================================================
Quality Checks
=======================================================================================
Script Purpose:
  This script performs various quality checks for data consistency , accuracy,
  and standardization across the 'silver' schemas . It includes checks for: 
  - Null or duplicate primary keys
  -Unwanted spaces in string fields
  -Data standardization and consistency
  -Invalid date ranges and orders
  -Data consistency between related fields

Usage Notes:
  - Run these checks after data loading Silver Layer.
  - Investigate and resolve any discrepancies found during the checks.
==========================================================================================
*/
--Check for Nulls or Duplicates in Primary Key
--Expectation: No Result

SELECT cst_id,COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Check for unwanted Spaces
--Expectation: No Result
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

--Check for NULLS or Negative Numbers
--Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL


-- Check for Data standardization & Consistency
SELECT DISTINCT prd_line 
FROM bronze.crm_prd_info

--Check for Invalid Date Orders
SELECT *
FROM bronze.crm_cust_info
WHERE prd_end_dt < prd_start_dt


--Check for Invalid Dates
SELECT 
NULLIF(sls_order_dt,0) as sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101

select 
sls_order_dt,
sls_ship_dt,
sls_due_dt
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt


-- Check for date errors


select BDATE
from bronze.erp_cust_az12
where BDATE < '1924-01-01' or BDATE > GETDATE()
