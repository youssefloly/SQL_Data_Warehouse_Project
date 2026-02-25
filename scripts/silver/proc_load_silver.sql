/*
==============================================================================
Stored Purpose: Load Silver Layer (Bronze -> Silver)
==============================================================================
Script Purpose :
  This stored procedure performs the ETL ( Extract , Transform , Load ) process to 
  populate the 'silver' schema tables from the 'bronze' schema
Actions Performed:
  - Truncates Silver tables
  - Inserts transformed and Cleaned data from Bronze into Silver tables
Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.


Usage Example :
  EXEC Silver.load_silver
============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
DECLARE @Start DATETIME , @End DATETIME,@Start_Time DATETIME , @End_Time DATETIME
BEGIN
-- insert data into silver.crm_cust_info
	BEGIN TRY
		SET @Start = GETDATE()
		PRINT'=======================================';
		PRINT'Loading Silvers Layer';
		PRINT'=======================================';

		PRINT'---------------------------------------';
		PRINT'Loading Tables From CRM';
		PRINT'---------------------------------------';
		SET @Start_Time = GETDATE()
		PRINT'--> TRUNCATE THE TABLE FIRST'
		TRUNCATE TABLE silver.crm_cust_info
		PRINT'--> INSERT THE DATA'
		INSERT INTO silver.crm_cust_info (cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)

		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 ELSE 'n/a'
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END cst_gndr,
		cst_create_date
		FROM ( SELECT *,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		)t
		WHERE flag_last = 1

--delete null values from the table
		PRINT '--> DELETE NULL VALUE'
		delete from silver.crm_cust_info
		where cst_id is null
		SET @END_Time = GETDATE()
		PRINT'---->Loading Time ' + CAST(DateDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' SECONDS'
		PRINT'########################################################'

--########################################
--insert data into silver.crm_prd_info
		SET @Start_Time = GETDATE()
		PRINT'--> TRUNCATE THE TABLE FIRST'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT'--> INSERT THE DATA'
		INSERT INTO silver.crm_prd_info (prd_id,prd_key,cat_id,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other Sales'
			 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			 ELSE 'n/a'
		END AS prd_line,
		prd_start_dt,
		prd_end_dt
		FROM bronze.crm_prd_info
		SET @End_Time = GETDATE()
		PRINT'---->Loading Time ' + CAST(DateDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' SECONDS'
		PRINT'########################################################'
--##############################################
--insert data into silver.crm_sales_details
		SET @Start_Time = GETDATE()
		PRINT'--> TRUNCATE THE TABLE FIRST'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT'--> INSERT THE DATA'
		INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price)

		SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE
			WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE) 
		END AS sls_order_dt,
		CASE
			WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE) 
		END AS sls_ship_dt,
		CASE
			WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE) 
		END AS sls_due_dt,
		CASE 
			WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != ABS(sls_price) * sls_quantity
			THEN ABS(sls_price) * sls_quantity 
			else sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE 
			WHEN sls_price <=0 OR sls_price IS NULL OR sls_price != ABS(sls_sales) / sls_quantity 
			THEN ABS(sls_sales) / nullif(sls_quantity,0)
			ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details
		SET @End_Time = GETDATE()
		PRINT'---->Loading Time ' + CAST(DateDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' SECONDS'
		PRINT'########################################################'
--########################################
		PRINT'---------------------------------------';
		PRINT'Loading Tables From ERP';
		PRINT'---------------------------------------';
--insert data into silver.erp_cust_az12
		SET @Start_Time = GETDATE()
		PRINT'--> TRUNCATE THE TABLE FIRST'
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT'--> INSERT THE DATA'
		INSERT INTO silver.erp_cust_az12 (CID,BDATE,GEN)

		SELECT 
		CASE
			WHEN LEN(CID) > 10 THEN SUBSTRING(CID,4,LEN(CID)) 
			ELSE CID
		END AS CID,
		CASE 
			WHEN BDATE > GETDATE() THEN NULL
			ELSE BDATE
		END AS BDATE,
		CASE 
			WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
			ELSE 'N/A'
		END AS GEN
		FROM bronze.erp_cust_az12
		SET @End_Time = GETDATE()
		PRINT'---->Loading Time ' + CAST(DateDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' SECONDS'
		PRINT'########################################################'
--########################################
--insert data into silver.erp_loc_a101
		SET @Start_Time = GETDATE()
		PRINT'--> TRUNCATE THE TABLE FIRST'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT'--> INSERT THE DATA'
		INSERT INTO silver.erp_loc_a101 (CID,CNTRY)
		SELECT 
		REPLACE(CID,'-','') AS CID,
		CASE
			WHEN TRIM(CNTRY) = 'DE'  THEN 'Germany'
			WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
			WHEN TRIM(CNTRY) ='' OR TRIM(CNTRY) IS NULL THEN 'N/A'
			ELSE CNTRY
		END AS CNTRY
		FROM bronze.erp_loc_a101
		SET @End_Time = GETDATE()
		PRINT'---->Loading Time ' + CAST(DateDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' SECONDS'
		PRINT'########################################################'
--########################################
--insert data into silver.erp_px_cat_g1v2
		SET @Start_Time = GETDATE()
		PRINT'--> TRUNCATE THE TABLE FIRST'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT'--> INSERT THE DATA'
		INSERT INTO silver.erp_px_cat_g1v2 (ID,CAT,SUBCAT,MAINTENANCE)

		SELECT *
		FROM bronze.erp_px_cat_g1v2
		SET @End_Time = GETDATE()
		PRINT'---->Loading Time ' + CAST(DateDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' SECONDS'
	END TRY
	BEGIN CATCH
		PRINT'There is an Error'
	END CATCH
END
SET @End = GETDATE()
PRINT'------>Loading Time For the Silver Layer ' + CAST(DateDIFF(SECOND,@Start,@End) AS NVARCHAR) + ' SECONDS'




--EXEC silver.load_silver
