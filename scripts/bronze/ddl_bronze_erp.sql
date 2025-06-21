/*how to validate object exist in database 
IF OBJECT_ID ('table_name', 'object_type') IS NOT NULL
	DROP TABLE table_name;
	
	=======================================================
	DDL Script: Create Bronze Table (ERP)
	=======================================================
	Script Purpose:
		This script creates tables in the 'bronze' schema, dropping existing 
		tables if they already exist.
	Run this script to re-define the DDL structure of 'bronze' tables

*/

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);

