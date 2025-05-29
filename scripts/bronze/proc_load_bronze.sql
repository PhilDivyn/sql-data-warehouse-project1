/* 
==========================================================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==========================================================================================================================
Script Purpose:
This SP loads data into the bronze schema from external CSV files.
it perfroms the follwoing actions:
-Truncates the bronze table before loading data
- Uses the 'BULK INSERT' command to load data from CSV files to bronze tables

Parameters:
None.
This SP does not accept parameters or return any values

Usage Example:
EXEC bronze.load_bronze;
============================================================================================================================
****Loading data into tables in the Bronze Schema Layer Using BULK INSERT to load the data from the CRM & ERP csv files into the already created tables
in the bronze schema
--Because this bulk insert (loading data into the bronze layer tables) would be carried out on a daily basis to get fresh data, 
into the datawarehouse, I have created a Stored Procedure to run daily.
*/

Exec bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.Load_bronze AS--- I have put it in the bronze layer and its the load scripts)
BEGIN
DECLARE @Start_time DATETIME, @End_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
BEGIN TRY
SET @batch_start_time = GETDATE();

			PRINT'========================================================';
			PRINT'Loading Bronze Layer';
			PRINT'========================================================';
			SET @Start_time = GETDATE();
			PRINT'*******Truncating table bronze.crm_cust_info***********';
				TRUNCATE TABLE bronze.crm_cust_info
			PRINT'---------------------------------------------------------';
			PRINT'Loading the CRM tables';
			PRINT'========================================================';
				BULK INSERT bronze.crm_cust_info
			from 'C:\Users\USER\OneDrive - London South Bank University\Documents\SQL Server Management Studio\DataWarehouseProject\datasets\source_crm\cust_info.csv'
			WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
			)
			SET @End_time = GETDATE();
			PRINT '>> LOAD DURATION:' + CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) +' seconds';
			PRINT '-----------------------------'

			SET @Start_time = GETDATE();
			PRINT'*******Truncating table bronze.crm_prd_info***********';
			TRUNCATE TABLE bronze.crm_prd_info;

			PRINT'***Inserting data into bronze.crm_prd_info from the path stated below****'
			BULK INSERT bronze.crm_prd_info
			from 'C:\Users\USER\OneDrive - London South Bank University\Documents\SQL Server Management Studio\DataWarehouseProject\datasets\source_crm\prd_info.csv'
			WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
			)
			SET @End_time = GETDATE();
			PRINT '>> LOAD DURATION:' + CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) +' seconds';
			PRINT '-----------------------------'

			SET @Start_time = GETDATE();
			PRINT'*******Truncating table bronze.crm_sales_info***********';
			TRUNCATE TABLE bronze.crm_sales_info

			PRINT'***Inserting data into bronze.crm_sales_info from the path stated below****' 
			BULK INSERT bronze.crm_sales_info
			from 'C:\Users\USER\OneDrive - London South Bank University\Documents\SQL Server Management Studio\DataWarehouseProject\datasets\source_crm\sales_info.csv'
			WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
			);
			SET @End_time = GETDATE();
			PRINT '>> LOAD DURATION:' + CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) +' seconds';
			PRINT '-----------------------------'

			---LOADING DATA INTO THE ERP TABLES FROM ERP CSV FILES
			PRINT'---------------------------------------------------------';
			PRINT'Loading the ERP tables';
			PRINT'========================================================';

			SET @Start_time = GETDATE();
			PRINT'*******Truncating table bronze.erp_px_cat_g1v2***********';
			TRUNCATE TABLE bronze.erp_px_cat_g1v2

			PRINT'***Inserting data into bronze.erp_px_cat_g1v2 from the path stated below****' 
			BULK INSERT bronze.erp_px_cat_g1v2
			from 'C:\Users\USER\OneDrive - London South Bank University\Documents\SQL Server Management Studio\DataWarehouseProject\datasets\source_erp\px_cat_g1v2.csv'
			WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
			);
			SET @End_time = GETDATE();
			PRINT '>> LOAD DURATION:' + CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) +' seconds';
			PRINT '-----------------------------'

			SET @Start_time = GETDATE();
			PRINT'*******Truncating table bronze.erp_loc_a101***********';
			TRUNCATE TABLE bronze.erp_loc_a101;
	
			PRINT'***Inserting data into bronze.erp_loc_a101 from the path stated below****' 
			BULK INSERT bronze.erp_loc_a101
			from 'C:\Users\USER\OneDrive - London South Bank University\Documents\SQL Server Management Studio\DataWarehouseProject\datasets\source_erp\LOC_A101.csv'
			WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
			);
			SET @End_time = GETDATE();
			PRINT '>> LOAD DURATION:' + CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) +' seconds';
			PRINT '-----------------------------'


			SET @Start_time = GETDATE();
			PRINT'*******Truncating table bronze.erp_cust_az12***********';
			TRUNCATE TABLE bronze.erp_cust_az12;
			PRINT'***Inserting data into bronze.erp_cust_az12 from the path stated below****' 
			BULK INSERT bronze.erp_cust_az12
			from 'C:\Users\USER\OneDrive - London South Bank University\Documents\SQL Server Management Studio\DataWarehouseProject\datasets\source_erp\cust_az12.csv'
			WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
			);
			SET @End_time = GETDATE();

			PRINT '>> LOAD DURATION:' + CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) +' seconds';
			PRINT '-----------------------------'
			
			SET @batch_end_time = GETDATE();
			PRINT'=================================================================='
			PRINT'LOADING BRONZE LAYER COMPLETED';
			PRINT'	- Total Load Duration:' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time)  AS NVARCHAR) + ' seconds'
			PRINT'==================================================================='
		END TRY
		BEGIN CATCH
			PRINT '======================================================';--(Track ETL Duration)Helps identify bottlenecks, optimise performance, monitor trends and detect issues.
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER ';
			PRINT 'Error message' + ERROR_MESSAGE();
			PRINT 'Error message' + CAST (ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error message' + CAST (ERROR_STATE() AS NVARCHAR);
			PRINT'======================================================='
			END CATCH
		END
