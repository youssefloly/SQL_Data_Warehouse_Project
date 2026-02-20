==============================================
DDL Script: Create Bronze Tables
==============================================
Script Purpose:
  This script creates tables in the 'bronze' schema ,dropping existing tables
    if they already exist.
  Run this script to re-define the DDL structure of 'bronze' Tables
==============================================
    
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
DECLARE @Start DATETIME , @End DATETIME
SET @Start = GETDATE()
BEGIN
	DECLARE @Start_Time DATETIME , @End_Time DATETIME
	BEGIN TRY
		PRINT'=======================================';
		PRINT'Loading Bronze Layer';
		PRINT'=======================================';

		PRINT'---------------------------------------';
		PRINT'Loading Tables From CRM';
		PRINT'---------------------------------------';
		SET @Start_Time = GETDATE()
		PRINT'--> Truncate Table : crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT'--> Insert Data into Table : crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\depi 2\DATA WAREHOUSE BARAA\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK)
		SET @End_Time = GETDATE()
		PRINT'---->Loading Time ' + CAST(DateDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' SECONDS'
		PRINT'########################################################'
		--SELECT COUNT(*) FROM bronze.crm_cust_info

		------------------------------------
		SET @Start_Time = GETDATE()
		PRINT'--> Truncate Table : crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT'--> Insert Data into Table : crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\depi 2\DATA WAREHOUSE BARAA\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK)
		SET @End_Time = GETDATE()
		PRINT'---->Loading Time ' + CAST(DateDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' SECONDS'
		PRINT'########################################################'

		--SELECT COUNT(*) FROM bronze.crm_prd_info
		-------------------------------------------------
		SET @Start_Time = GETDATE()
		PRINT'--> Truncate Table : crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT'--> Insert Data into Table : crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\depi 2\DATA WAREHOUSE BARAA\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK)
		SET @End_Time = GETDATE()
		PRINT'---->Loading Time ' + CAST(DateDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' SECONDS'
		PRINT'########################################################'
		--SELECT COUNT(*) FROM bronze.crm_sales_details
		----------------------------------------------------
		PRINT'---------------------------------------';
		PRINT'Loading Tables From ERP';
		PRINT'---------------------------------------';
		SET @Start_Time = GETDATE()
		PRINT'--> Truncate Table : erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT'--> Insert Data into Table : erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\depi 2\DATA WAREHOUSE BARAA\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK)
		SET @End_Time = GETDATE()
		PRINT'---->Loading Time ' + CAST(DateDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' SECONDS'
		PRINT'########################################################'
		--SELECT COUNT(*) FROM bronze.erp_cust_az12
		------------------------------
		SET @Start_Time = GETDATE()
		PRINT'--> Truncate Table : erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT'--> Insert Data into Table : erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\depi 2\DATA WAREHOUSE BARAA\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK)
		SET @End_Time = GETDATE()
		PRINT'---->Loading Time ' + CAST(DateDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' SECONDS'
		PRINT'########################################################'
		--SELECT COUNT(*) FROM bronze.erp_loc_a101
		-----------------------------------
		SET @Start_Time = GETDATE()
		PRINT'--> Truncate Table : erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT'--> Insert Data into Table : erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\depi 2\DATA WAREHOUSE BARAA\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK)
		SET @End_Time = GETDATE()
		--SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2
		PRINT'---->Loading Time ' + CAST(DateDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' SECONDS'
		PRINT'########################################################'
	END TRY
	BEGIN CATCH
		PRINT'There is an Error'
	END CATCH
END
SET @End = GETDATE()
PRINT'---->Loading Time For the Bronze Layer ' + CAST(DateDIFF(SECOND,@Start,@End) AS NVARCHAR) + ' SECONDS'

GO
EXEC bronze.load_bronze
