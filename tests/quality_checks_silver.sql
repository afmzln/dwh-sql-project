/*
==============================================================
DATA QUALITY CHECKS - SILVER LAYER
==============================================================
Purpose:
- Identifies data quality issues across all tables before and after transformation.
- Ensures data integrity when moving from bronze to silver layer.
- It includes checks for:
    - Null or duplicates primary keys.
    - Unwanted spaces in string fields.
    - Data standadization and consitency.
    - Invalid data ranges and orders.
    - Data consistency between related fields.

Implementation Notes:
1. Each section checks a specific table
2. Queries return:
   - Empty result = Quality check passed
   - Rows returned = Issues needing investigation
3. Checks are designed to run post-transformation(silver) or pre-transformation(bronze)

*/

/* 
==============================================================
TABLE: silver.crm_cust_info - Customer Information
============================================================== 
*/
-- Primary Key Validation
--Checks for: Null values, duplicate customer IDs
--Expected: Empty result set
	SELECT 
	cst_id,
	COUNT (*)
	FROM silver.crm_cust_info
	GROUP BY cst_id
	HAVING COUNT (*) > 1


-- String Formatting Checks
-- Checks for: Leading/trailing spaces in name fields
-- Expected: 0 rows returned
	 SELECT cst_firstname
	 FROM silver.crm_cust_info
	 WHERE cst_firstname != TRIM (cst_firstname)
----------------------------------------------------------------------
	 SELECT cst_lastname
	 FROM silver.crm_cust_info
	 WHERE cst_lastname != TRIM (cst_lastname)

 -- Data Standardization & Consistency
 SELECT DISTINCT cst_gndr
 FROM silver.crm_cust_info
 --------------------------------------------------
 SELECT DISTINCT cst_marital_status
 FROM silver.crm_cust_info
 --------------------------------------------------------------------------------------------------------------------------------
 
/* 
==============================================================
TABLE: silver.crm_prd_info - Product Information
============================================================== 
*/
-- Primary Key Validation 
-- check for nulls or duplicates in PK
-- Expected: Empty result (no duplicates/null keys)
	SELECT prd_id,
	COUNT(*)
	FROM silver.crm_prd_info
	GROUP BY prd_id
	HAVING COUNT(*) > 1 OR prd_id IS NULL
-------------------------------------------------------------------------------------
-- Product Name Formatting 
-- Check for unwanted spaces
-- Expectations: No Results
	SELECT prd_nm
    FROM silver.crm_prd_info
    WHERE prd_nm != TRIM(prd_nm)
-------------------------------------------------------------------------------------
-- Check for NULLs or Negative Numbers
-- Price Validation
-- Expectations: No Results
	 SELECT prd_cost
     FROM silver.crm_prd_info
     WHERE prd_cost < 0  OR prd_cost IS NULL
-------------------------------------------------------------------------------------
-- Data Standardization & Consistency
-- Product Line Consistency
	SELECT DISTINCT prd_line
	FROM silver.crm_prd_info
--------------------------------------------------------------------------------------
-- Check for Invalid Date Orders
-- Date Logic Validation
	SELECT * FROM silver.crm_prd_info
	WHERE  prd_end_dt < prd_start_dt

 --------------------------------------------------------------------------------------------------------------------------------
 
/* 
==============================================================
TABLE: silver.crm_sales_details - Sales Transactions
============================================================== 

Key Quality Rules:
1. Sales = Quantity × Price (must be positive)
2. Dates must be valid and properly sequenced
3. All foreign keys must reference valid records

ABS() -  Returns absolute value of a number 
*/

-- Check for Unwanted Spaces
-- Referential Integrity Checks
-- Expectations: No Results
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)
---------------------------------------------------------------------------
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN ( SELECT prd_key FROM silver.crm_prd_info)
---------------------------------------------------------------------------
SELECT
*
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN ( SELECT cst_id FROM silver.crm_cust_info)
---------------------------------------------------------------------------
-- Date Validation
--Check For Invalid Dates: sls_order_dt, sls_ship_dt,sls_due_dt
SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101 -- check for outlier by validating the boundaries of the date range
---------------------------------------------------------------------------
-- Check for Invalid Date Orders
-- Date Sequence Validation
SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt >sls_due_dt
--------------------------------------------------------------------------
-- Financial Consistency Check: sls_sales, sls_quantity, sls_price
SELECT DISTINCT
sls_sales AS old_sls_sales, 
sls_quantity, 
sls_price AS old_sls_price,
CASE WHEN  sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
             THEN sls_quantity * ABS(sls_price)
     ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price  IS NULL OR sls_price  <= 0
            THEN sls_sales / NULLIF(sls_quantity ,0)
      ELSE sls_price  
END AS sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR  sls_price IS NULL
OR sls_sales <= 0  OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

/* 
==============================================================
TABLE: silver.erp_cust_az12 - ERP Customer Data
============================================================== 
*/
-- ID Formatting and Matching
 SELECT 
cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE cid LIKE '%AW00011000%'

SELECT * FROM silver.crm_cust_info --to compare the info
--To check/compare and clean the ID that did not match the other id
SELECT 
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
          ELSE cid
END  AS cid, 
bdate,
gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
          ELSE cid
END  NOT IN( SELECT DISTINCT cst_key FROM silver.crm_cust_info)
--------------------------------------------------------------------------
--Identify Out-of-Range-Dates
-- Date Validation
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < ' 1924-01-01' OR bdate > GETDATE()
--------------------------------------------------------------------------
-- Data Standardization & Consistency
-- Gender Standardization Check
SELECT DISTINCT gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
             ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12
/* 
==============================================================
TABLE: silver.erp_loc_a101 - Customer Locations
============================================================== 
*/
SELECT
cid,
cntry
FROM bronze.erp_loc_a101;

SELECT cst_key FROM silver.crm_cust_info;
--------------------------------------------------------------------------
-- ID Matching Check
SELECT
REPLACE(cid,'-', '') cid,
cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid,'-', '') NOT IN
(SELECT cst_key FROM silver.crm_cust_info)
--------------------------------------------------------------------------
--Data Standardization & Consistency
SELECT DISTINCT 
cntry
FROM bronze.erp_loc_a101
ORDER BY cntry
/* 
==============================================================
TABLE: silver.erp_px_cat_g1v2 - Product Categories
============================================================== 
*/
-- String Formatting Check
--Check for unwanted spaces

SELECT *FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat  != TRIM(subcat ) OR maintenance  != TRIM(maintenance) 
-------------------------------------------------------------------------------------------
--Check data standardization and consistency : cat, subcat, maintenances
-- Category Value Review
SELECT  DISTINCT
cat
FROM bronze.erp_px_cat_g1v2
