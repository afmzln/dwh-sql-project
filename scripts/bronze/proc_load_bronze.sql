/* Method to load data from source to data warehouse is BULK INSERT
	BULK INSERT - method of loading massive amt of data very quickly from csv files/txt file directly into database
	TRUNCATE - Quickly delete all rows frm a table, resetting it to an empty state
	PRINT - to track  execution,debug issues, and understand its flow
	TABLOCK -locking entire table during loading
	TRY...CATCH - ensures error handling,data integrity, and issue logging for easier debugging
	Track ETL Duration - Helps to identify bottlenecks, optimize performance, monitor trends, detect issues
	DATEDIFF() - calculate the difference btwn 2 dates,return days,months or years

	===============================================================================
	Stored Procedure: Load Bronze Layer (Source -> Bronze)
	===============================================================================
	Script Purpose:
		This stored procedure loads data into the 'bronze' schema from external CSV files.
		It perform the following actions:
			BULK INSERT - method of loading massive amount of data  from csv files/txt file directly into database (bronze tables)
			TRUNCATE - Quickly delete all rows frm a table, resetting it to an empty state
			PRINT - to track  execution,debug issues, and understand its flow
			TABLOCK -locking entire table during loading
			TRY...CATCH - ensures error handling,data integrity, and issue logging for easier debugging
			Track ETL Duration - Helps to identify bottlenecks, optimize performance, monitor trends, detect issues
			DATEDIFF() - calculate the difference btwn 2 dates,return days,months or years
		Parameters:
			None. This stored procedure does not accept any parameters or return any values.

		Usage Example:
			EXEC bronze.load_bronze 
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE ();
		PRINT '===============================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '===============================================';

		PRINT '**********************************************';
		PRINT 'Loading CRM Tables';
		PRINT '**********************************************';

		SET @start_time = GETDATE ();
			-- Truncating Table:bronze.crm_cust_info--
		TRUNCATE TABLE bronze.crm_cust_info
			-- Inserting Data Into :bronze.crm_cust_info--
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\LENOVO\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE ();
		PRINT 'Load Duration :' +  CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) +' '+ 'seconds';
------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE ();
		-- Truncating Table:bronze.crm_prd_info--
		TRUNCATE TABLE bronze.crm_prd_info
			-- Inserting Data Into :bronze.crm_prd_info--
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\LENOVO\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE ();
		PRINT 'Load Duration :' +  CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) +' '+ 'seconds';
-------------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE ();
		-- Truncating Table:bronze.crm_sales_details--
		TRUNCATE TABLE bronze.crm_sales_details
			-- Inserting Data Into :bronze.crm_sales_details--
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\LENOVO\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE ();
		PRINT 'Load Duration :' +  CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) +' '+ 'seconds';
----------------------------------------------------------------------------------------------------------
		PRINT '**********************************************';
		PRINT 'Loading ERP Tables';
		PRINT '**********************************************';

		SET @start_time = GETDATE ();
			-- Truncating Table:bronze.erp_cust_az12--
		TRUNCATE TABLE bronze.erp_cust_az12
			-- Inserting Data Into :bronze.erp_cust_az12--
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\LENOVO\Downloads\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE ();
		PRINT 'Load Duration :' +  CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' '+ 'seconds';
-------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE ();
		-- Truncating Table:bronze.erp_loc_a101--
		TRUNCATE TABLE bronze.erp_loc_a101
			-- Inserting Data Into :bronze.erp_loc_a101--
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\LENOVO\Downloads\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE ();
		PRINT 'Load Duration :' +  CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' '+ 'seconds';
----------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE ();
		-- Truncating Table:bronze.erp_px_cat_g1v2--
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
			-- Inserting Data Into :bronze.erp_px_cat_g1v2--
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\LENOVO\Downloads\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE ();
		PRINT 'Load Duration :' +  CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) +' '+  'seconds';
-------------------------------------------------------------------------------------------------------------
		SET @batch_end_time = GETDATE ();
		PRINT '===============================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT ' Total Load Duration :' +  CAST(DATEDIFF(second, @batch_start_time, @batch_end_time)AS NVARCHAR) +' '+  'seconds';
		PRINT '===============================================';
	END TRY
	BEGIN CATCH
		PRINT '===============================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT '===============================================';
	END CATCH
END
  
