/*
========================================================
DATA TRANSFORMATION: Bronze → Silver Layer Load
========================================================

Purpose:
  This stored procedure performs the ETL ( Extract, Transform, Load) process. 
  Transforms raw source data (bronze layer) into analysis-ready tables (silver layer) by:
  - Cleaning inconsistent data (NULL handling, trimming, invalid values)
  - Standardizing coded values (gender, marital status, product lines)
  - Enriching with business-friendly labels
  - Applying data integrity checks
Actions Performed:
  - Truncates Silver Table
  - Insert transformed and cleansed data from Bronze into Silver tables.

Key SQL Functions Used:
  - TRIM()          - Removes leading/trailing whitespace
  - UPPER()         - Ensures case consistency
  - SUBSTRING()     - Extracts parts of strings
  - REPLACE()       - Modifies string patterns
  - ISNULL()        - Handles NULL values
  - CASE WHEN       - Conditional value mapping
  - LEAD()          - Accesses next row in a window
  - CAST()          - Converts data types
  - NULLIF()        - Prevents division by zero

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
This query demonstrates multiple data transformation techniques:
1. DATA CLEANSING:
   - Remove duplicates: 
     - ROW_NUMBER() to keep only most recent record per customer
   - Data filtering: 
     - WHERE cst_id IS NOT NULL (removes null customer IDs)
   - Handling unwanted spaces: 
     - TRIM() on name fields (cst_firstname, cst_lastname)
   - Handling invalid values: 
     - ELSE 'n/a' for unrecognized gender/marital codes
2. DATA NORMALIZATION & STANDARDIZATION:
   - Standardize gender values: 
     - 'F'/'M' → 'Female'/'Male' (with UPPER(TRIM()) for consistency)
   - Standardize marital status: 
     - 'S'/M' → 'Single'/'Married'
3. BUSINESS RULES & LOGIC:
   - Most recent record logic: 
    - PARTITION BY cst_id ORDER BY cst_create_date DESC
   - Default value handling: 
    - 'n/a' for unrecognized codes
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
        TRIM (cst_firstname) AS cst_firstname,
        TRIM (cst_lastname) AS cst_lastname,
        CASE WHEN UPPER(TRIM(cst_marital_status)) ='S' THEN 'Single'
             WHEN UPPER(TRIM(cst_marital_status)) ='M' THEN 'Married'
             ELSE 'n/a'
        END cst_marital_status,
        CASE WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'Female'
             WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'Male'
             ELSE 'n/a'
        END cst_gndr,
        cst_create_date
        FROM (
         SELECT *,
         ROW_NUMBER() OVER ( PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
     FROM bronze.crm_cust_info
     WHERE cst_id IS NOT NULL
     )t WHERE flag_last = 1 
     --SELECT * FROM silver.crm_cust_info
     SET @end_time = GETDATE ();
     PRINT 'Load Duration :' +  CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) +' '+ 'seconds';
--------------------------------------------------------------------------------------------------------------------------------------
/*
===================================================================
DATA TRANSFORMATION: silver.crm_prd_info
====================================================================
This query demonstrates multiple data transformation techniques:
1. DATA CLEANSING:
   - Handling missing data: 
     * ISNULL(prd_cost,0) - Replaces NULL costs with 0
  - Data type casting: 
    * CAST(prd_start_dt AS DATE) - Ensures proper date format
2. DATA NORMALIZATION & STANDARDIZATION:
   - Standardizes product line codes:
* 'M' → 'Mountain'
* 'R' → 'Road'
* 'S' → 'Other Sales'
* 'T' → 'Touring'
- Normalizes text with UPPER(TRIM())
3. DERIVED COLUMNS:
   - Creates cat_id by:
     * Extracting first 5 chars of prd_key
     * Replacing '-' with '_'
   - Creates prd_key by:
     * Extracting from position 7 to end
   - Calculates prd_end_dt using:
     * LEAD() to get next product date
     * Subtracts 1 day for valid end date
4. BUSINESS RULES & LOGIC:
   - Product lifecycle logic:
     * End date = next start date - 1 day
   - Default value handling:
     * 'n/a' for unrecognized product lines
   - Cost validation:
     * Ensures no NULL costs in final data
5. DATA INTEGRATION:
   - Prepares keys (cat_id, prd_key) to match with:
     * erp_px_cat_g1v2 (category reference)
     * crm_sales_details (product sales)
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
       REPLACE(SUBSTRING( prd_key,   1,   5 ) ,  '-',    '_'  ) AS cat_id, -- check match info in erp_px_cat_g1v2 table, extract category ID
       SUBSTRING( prd_key,   7,  LEN(prd_key)) AS prd_key, --check match info in crm_sales_details table, extract product key
       prd_nm,
       ISNULL (prd_cost,0) AS prd_cost,
       CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
       END AS prd_line, -- map product line codes to descriptive values
       CAST(prd_start_dt AS DATE) AS prd_start_dt,
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
			This query demonstrates comprehensive sales data processing:

			1. DATA CLEANSING:
			   - Handling invalid dates:
				 * Filters out invalid date formats (LEN != 8 or value = 0)
				 * Converts valid dates from VARCHAR to DATE
			   - Handling missing/invalid values:
				 * Recalculates sales when NULL/negative (sls_quantity * ABS(sls_price))
				 * Derives price when invalid (sls_sales/NULLIF(sls_quantity,0))
			   - Data type casting:
				 * CAST operations for proper date formatting

			2. BUSINESS RULES & LOGIC:
			   - Financial validation:
				 * Ensures sales = quantity * price
				 * Uses ABS() to handle negative prices
				 * NULLIF prevents division by zero
			   - Date validation:
				 * Strict 8-character format requirement
				 * NULLs for invalid entries
						3. DERIVED COLUMNS:
			   - Creates validated versions of:
				 * sls_order_dt
				 * sls_ship_dt
				 * sls_due_dt
				 * sls_sales
				 * sls_price

			4. DATA NORMALIZATION:
			   - Standardizes date formats
			   - Normalizes financial calculations
			   - Ensures consistent NULL handling

			5. DATA INTEGRATION:
			   - Maintains referential integrity with:
				 * sls_prd_key links to products
				 * sls_cust_id links to customers
			Quality Assurance:
			- All financial calculations are validated
			- Invalid dates are explicitly handled
			- Edge cases (zero quantities) are addressed

			Note: This transformation creates reliable sales data for analysis,
			enforcing strict quality checks on dates and financial values.
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
			CASE WHEN sls_order_dt = 0  OR LEN(sls_order_dt) != 8 THEN NULL
						ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE WHEN sls_ship_dt= 0  OR LEN(sls_ship_dt)  != 8 THEN NULL
						ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE WHEN sls_due_dt= 0  OR LEN(sls_due_dt)  != 8 THEN NULL
						ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE WHEN  sls_sales IS NULL OR sls_sales <= 0 OR sls_sales  != sls_quantity * ABS(sls_price)
						 THEN sls_quantity * ABS(sls_price)
				 ELSE sls_sales
			END AS sls_sales, --recalculate sales if original value is missing or incorrect
			sls_quantity,
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

			This query demonstrates customer data standardization with these techniques:

			1. DATA CLEANSING:
			   - Handling invalid values:
				 * Filters future birthdates (bdate > GETDATE() → NULL)
			   - Data normalization:
				 * Removes 'NAS' prefix from customer IDs
				 * TRIM() + UPPER() for consistent gender values

			2. DATA NORMALIZATION & STANDARDIZATION:
			   - Standardizes gender values:
				 * 'F'/'FEMALE' → 'Female'
				 * 'M'/'MALE' → 'Male'
				 * All others → 'n/a'
			   - Consistent ID formatting:
				 * Removes 'NAS' prefix while preserving original IDs
			3. DERIVED COLUMNS:
			   - Creates cleaned customer ID (cid)
			   - Creates validated birthdate (bdate)
			   - Creates standardized gender (gen)

			4. BUSINESS RULES & LOGIC:
			   - Data validation:
				 * Impossible future dates become NULL
			   - Default handling:
				 * 'n/a' for unrecognized genders
			   - ID standardization:
				 * Business rule for NAS-prefixed IDs

			5. DATA INTEGRATION:
			   - Prepares customer IDs to match with:
				 * crm_cust_info table
				 * erp_loc_a101 table
			
			Execution Notes:
			- Maintains all original records while improving data quality
			- Uses CASE statements for clear business logic
			- Preserves data integrity through non-destructive transformations

			Quality Assurance:
			- Future dates explicitly handled
			- Gender values normalized to 3 possible values
			- Customer IDs maintain referential integrity
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
			CASE WHEN bdate > GETDATE() THEN NULL
				 ELSE bdate
			END AS bdate, -- Set future birthdates to NULL
			CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
						WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
						 ELSE 'n/a'
			END AS gen --Normalize gender values and handle unknown cases
			FROM bronze.erp_cust_az12

			--SELECT * FROM silver.erp_cust_az12
			SET @end_time = GETDATE ();
			PRINT 'Load Duration :' +  CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) +' '+ 'seconds';
----------------------------------------------------------------------------------------------------------
			/*
			===================================================================
			DATA TRANSFORMATION: silver.erp_loc_a101 (Customer Locations)
			====================================================================

			This query demonstrates geographic data standardization with:

			1. DATA CLEANSING:
			   - Handling invalid values:
				 * REPLACE() removes hyphens from customer IDs
				 * TRIM() cleans country code whitespace
			   - Handling missing data:
				* Empty/NULL countries → 'n/a'
			2. DATA NORMALIZATION & STANDARDIZATION:
			   - Standardizes country names:
				 * 'DE' → 'Germany'
				 * 'US/USA' → 'United States'
				 * Blank/NULL → 'n/a'
			   - Consistent ID formatting:
				 * Hyphen removal from customer IDs
			3. DERIVED COLUMNS:
			   - Creates cleaned customer ID (cid)
			   - Creates standardized country name (cntry)
		    4. BUSINESS RULES & LOGIC:
			   - Geographic standardization:
				 * Official country naming conventions
			   - Default handling:
				 * 'n/a' for missing countries
			   - ID formatting:
				 * Business rule for hyphen removal

			5. DATA INTEGRATION:
			   - Prepares customer IDs to match with:
				 * erp_cust_az12 table
				 * crm_cust_info table

			Data Quality Features:
			- Ensures consistent country naming
			- Handles all edge cases (empty/NULL values)
			- Maintains referential integrity through ID cleaning
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
			REPLACE(cid,'-', '') cid,

			CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
						WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
						WHEN TRIM(cntry) = ''  OR cntry IS NULL THEN 'n/a'
						ELSE TRIM(cntry) 
			END AS cntry -- Normalize and handle missing or blank country codes
			FROM bronze.erp_loc_a101

			--SELECT * FROM silver.erp_loc_a101
			SET @end_time = GETDATE ();
			PRINT 'Load Duration :' +  CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) +' '+ 'seconds';
-----------------------------------------------------------------------------------------------------------------
			/*
			===================================================================
			DATA TRANSFORMATION: silver.erp_px_cat_g1v2 (Product Categories)
			====================================================================
			Techniques Applied: NO TRANSFORMATION OCCUR
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

