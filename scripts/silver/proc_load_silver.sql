/*
========================================================
DATA TRANSFORMATION: Bronze → Silver Layer Load
========================================================
Purpose:
  This stored procedure performs the ETL ( Extract, Transform, Load) process by transforms raw source data (bronze layer) into analysis-ready tables (silver layer)
Actions Performed:
  - Truncates Silver Table
  - Insert transformed and cleansed data from Bronze into Silver tables.

Key SQL Functions Used:
  - TRIM()          - Removes leading/trailing whitespace
  - UPPER()         - Converts text to uppercase for consistent comparison
  - ROW_NUMBER()    - Window function to identify the most recent record
  - SUBSTRING()     - Extracts parts of strings
  - REPLACE()       - Replaces hyphens with underscores
  - ISNULL()        - Handles NULL values
  - CASE WHEN       - Conditional logic for value transformation
  - LEAD()          - Window function to access next row's data
  - CAST()          - Type conversion between numeric, string, and date
  - ABS()			- Ensures positive price values
  - NULLIF()        - Prevents division by zero
  - LEN()			- Gets string length

 Usage Example:
   EXEC silver.load_silver

*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
  DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
  BEGIN TRY
     SET @batch_start_time = GETDATE ();
     PRINT '===============================================';
     PRINT 'LOADING SILVER LAYER';
     PRINT '===============================================';

     PRINT '**********************************************';
     PRINT 'Loading CRM Tables';
     PRINT '**********************************************';
 /*
===================================================================
DATA TRANSFORMATION: silver.crm_cust_info
====================================================================
Transformation Techniques:
 1. DATA CLEANING:
    - TRIM() whitespace from name fields
    - Filter NULL customer IDs      
  2. STANDARDIZATION:
     - Marital status: 'S'/'M' → 'Single'/'Married'
     - Gender: 'F'/'M' → 'Female'/'Male'   
  3. DEDUPLICATION:
    - Keep only most recent record per customer (ROW_NUMBER())
====================================================================
*/
     SET @start_time = GETDATE ();
     --Truncating Table: silver.crm_cust_info
       TRUNCATE TABLE silver.crm_cust_info;
    -- Inserting Data Into: silver.crm_cust_info
       INSERT INTO silver.crm_cust_info (
         cst_id,
         cst_key,
         cst_firstname,
         cst_lastname,
         cst_marital_status,
         cst_gndr,
         cst_create_date)
       SELECT 
         cst_id,
         cst_key,
	     TRIM (cst_firstname) AS cst_firstname,                     -- Clean first name by removing whitespace
         TRIM (cst_lastname) AS cst_lastname,                       -- Clean last name by removing whitespace
         CASE WHEN UPPER(TRIM(cst_marital_status)) ='S' THEN 'Single'
              WHEN UPPER(TRIM(cst_marital_status)) ='M' THEN 'Married'
              ELSE 'n/a'                                             -- Standardize unknown values
         END cst_marital_status,                                     -- Transform marital status codes to readable values
         CASE WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'Female'
              WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'Male'
              ELSE 'n/a'                                             -- Standardize unknown values
         END cst_gndr,                                               -- Transform gender codes to readable values
				cst_create_date
       FROM (
              SELECT 
              *,
              ROW_NUMBER() OVER ( PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last -- Identify most recent record per customer
              FROM bronze.crm_cust_info
              WHERE cst_id IS NOT NULL                               -- Filter out records without customer ID
        )t WHERE flag_last = 1                                        -- Select only the most recent record
         --SELECT * FROM silver.crm_cust_info
     SET @end_time = GETDATE ();
     PRINT 'Load Duration :' +  CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) +' '+ 'seconds';
----------------------------------------------------------------------------------------------------------
/*
===================================================================
DATA TRANSFORMATION: silver.crm_prd_info
====================================================================
Transformation Techniques:
 1. Data Cleaning:
    - `REPLACE()` hyphens with underscores in `prd_key` (category ID extraction)
   - `TRIM()` whitespace from `prd_line` codes  
   - Handle NULL costs with `ISNULL()` (default to 0)  
 2. Derived Columns:
   - Extract `cat_id` (first 5 chars of `prd_key`, hyphens → underscores)  
   - Extract clean `prd_key` (remaining chars after position 7)  
   - Map `prd_line` codes to full names (e.g., 'M' → 'Mountain')  

3. Data Standardization:
   - Convert `prd_start_dt` and `prd_end_dt` to `DATE` type  
   - Standardize "n/a" for unknown product lines  


====================================================================
*/
     SET @start_time = GETDATE ();
     -- Truncating Table: silver.crm_prd_info
        TRUNCATE TABLE silver.crm_prd_info;
     -- Inserting Data Into: silver.crm_prd_info
        INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,       
			prd_key,   
			prd_nm,  
			prd_cost,   
			prd_line,  
			prd_start_dt, 
			prd_end_dt)
		SELECT
			prd_id,
			REPLACE(SUBSTRING( prd_key,   1,   5 ) ,  '-',    '_'  ) AS cat_id,    -- Extract first 5 chars of product key and replace hyphens with underscores
			SUBSTRING( prd_key,   7,  LEN(prd_key)) AS prd_key, -- Extract product key (excluding category prefix)
			prd_nm,
			ISNULL (prd_cost,0) AS prd_cost,       -- Replace NULL cost values with 0
			CASE UPPER(TRIM(prd_line))             -- Transform product line codes to readable values
						WHEN 'M' THEN 'Mountain'
						WHEN 'R' THEN 'Road'
						WHEN 'S' THEN 'Other Sales'
						WHEN 'T' THEN 'Touring'
						 ELSE 'n/a'
			END AS prd_line, 
			CAST(prd_start_dt AS DATE) AS prd_start_dt,    -- Ensure proper date formatting
			CAST (LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt -- calculate end date as one day before the next start date
		FROM bronze.crm_prd_info

     --SELECT * FROM silver.crm_prd_info
     SET @end_time = GETDATE ();
     PRINT 'Load Duration :' +  CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) +' '+ 'seconds';
----------------------------------------------------------------------------------------------------------
/*
===================================================================
DATA TRANSFORMATION: silver.crm_sales_details
====================================================================
Transformation Techniques:
1. DATA CLEANING:
   - Handle invalid date formats (zero values or incorrect lengths)
   - Replace NULL or negative price values
   - Correct inconsistent sales amounts

2. DERIVED COLUMNS:
   - Recalculate sales amount when original is invalid
   - Derive unit price when missing or invalid
====================================================================
*/
	SET @start_time = GETDATE ();
	-- Truncating Table: silver.crm_sales_details';
	   TRUNCATE TABLE silver.crm_sales_details;
	--Inserting Data Into: silver.crm_sales_details';
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
	-- Convert numeric dates to DATE type after validation
			CASE WHEN sls_order_dt = 0  OR LEN(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE WHEN sls_ship_dt= 0  OR LEN(sls_ship_dt)  != 8 THEN NULL
			     ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE WHEN sls_due_dt= 0  OR LEN(sls_due_dt)  != 8 THEN NULL
			     ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
	  -- Validate and recalculate sales amount if needed
			CASE WHEN  sls_sales IS NULL OR sls_sales <= 0 OR sls_sales  != sls_quantity * ABS(sls_price)
			     THEN sls_quantity * ABS(sls_price)
				 ELSE sls_sales
			END AS sls_sales, 
			sls_quantity,
	-- Derive price if missing or invalid
			CASE WHEN sls_price  IS NULL OR sls_price  <= 0
			     THEN sls_sales / NULLIF(sls_quantity ,0)
			     ELSE sls_price  
			END AS sls_price -- Derive price if original value is invalid
		FROM bronze.crm_sales_details

	--SELECT * FROM silver.crm_sales_details
	SET @end_time = GETDATE ();
	PRINT 'Load Duration :' +  CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) +' '+ 'seconds';
----------------------------------------------------------------------------------------------------------

	PRINT '**********************************************';
	PRINT 'Loading ERP Tables';
	PRINT '**********************************************';	
/*
===================================================================
DATA TRANSFORMATION: silver.erp_cust_az12
====================================================================
Transformation Techniques:
1. DATA CLEANING:
   - Remove 'NAS' prefix from customer IDs when present
   - Handle invalid future birthdates by setting to NULL
   - TRIM() whitespace from gender values

2. STANDARDIZATION:
   - Gender values: 'F'/'FEMALE' → 'Female'
====================================================================
*/
	SET @start_time = GETDATE ();
	-- Truncating Table: silver.erp_cust_az12';
	   TRUNCATE TABLE silver.erp_cust_az12;
	--Inserting Data Into: silver.erp_cust_az12';
	   INSERT INTO silver.erp_cust_az12(
		 cid,
		 bdate,
		 gen)
	   SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) -- Remove ‘NAS’ prefix if present
			 ELSE cid
		END  AS cid, 
		CASE WHEN bdate > GETDATE() THEN NULL    -- Set future birthdates to NULL
			 ELSE bdate
		END AS bdate,  
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'   -- Standardize gender values (accepts both codes and full words)
			 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			 ELSE 'n/a'
		END AS gen 
	  FROM bronze.erp_cust_az12

	--SELECT * FROM silver.erp_cust_az12
	SET @end_time = GETDATE ();
	PRINT 'Load Duration :' +  CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) +' '+ 'seconds';
----------------------------------------------------------------------------------------------------------
/*
===================================================================
DATA TRANSFORMATION: silver.erp_loc_a101 (Customer Locations)
====================================================================
Transformation Techniques:
1. DATA CLEANING:
   - REPLACE() hyphens in customer IDs
   - TRIM() whitespace from country codes
   - Handle empty strings and NULL country values

2. STANDARDIZATION:
   - Country codes: 'DE' → 'Germany'
   - Blank/NULL → 'n/a'

====================================================================
*/
	SET @start_time = GETDATE ();
	-- Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
	--Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
			cid,
			cntry)
		SELECT
			REPLACE(cid,'-', '') cid,                    -- Remove all hyphens from customer ID
			CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'                       -- Normalize country codes to full names handle missing or blank country codes
				 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				 WHEN TRIM(cntry) = ''  OR cntry IS NULL THEN 'n/a'
				 ELSE TRIM(cntry)  -- Keep original value (trimmed) for unmapped countries
			END AS cntry 
		FROM bronze.erp_loc_a101
		--SELECT * FROM silver.erp_loc_a101
	SET @end_time = GETDATE ();
	PRINT 'Load Duration :' +  CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) +' '+ 'seconds';
-----------------------------------------------------------------------------------------------------------------
/*
===================================================================
DATA TRANSFORMATION: silver.erp_px_cat_g1v2 (Product Categories)
Note: No transformations applied - direct copy from bronze
====================================================================
*/
	SET @start_time = GETDATE ();
	-- Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
	-- Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
			)
		SELECT 
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2
		--SELECT * FROM silver.erp_px_cat_g1v2
	SET @end_time = GETDATE ();
	PRINT 'Load Duration :' +  CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) +' '+ 'seconds';
-------------------------------------------------------------------------------------------------------------------------------------
	SET @batch_end_time = GETDATE ();
		PRINT '===============================================';
		PRINT 'Loading Silver Layer is Completed';
		PRINT ' Total Load Duration :' +  CAST(DATEDIFF(second, @batch_start_time, @batch_end_time)AS NVARCHAR) +' '+  'seconds';
		PRINT '===============================================';
  END TRY
  BEGIN CATCH
		PRINT '===============================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT '===============================================';
  END CATCH
END

