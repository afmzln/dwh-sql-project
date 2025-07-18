/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Purpose:
    Establishes the analytical foundation by creating Gold layer views that form
    a star schema data warehouse structure. These views transform Silver layer
    data into business-ready dimensional models optimized for analytics.

Key Features:
    - Implements conforming dimensions and fact tables following the star schema best practices
    - Applies business logic and data cleansing rules
    - Enables direct querying for reporting and analytical purposes

Design Principles:
    - Standardized dimensional modeling approach
    - User-friendly naming conventions
    - Optimized for query performance
    - Maintains data lineage from Silver layer sources
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
-- Purpose: Create a clean, integrated customer dimension with business-friendly names
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customer AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    ci.cst_id                 AS customer_id,
    ci.cst_key                AS customer_number,
    ci.cst_firstname          AS first_name,
    ci.cst_lastname           AS last_name,
    la.cntry                  AS country,
    ci.cst_marital_status     AS marital_status,  -- Fixed space in original column name
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  -- CRM is the Master for gender info
        ELSE COALESCE(ca.gen, 'n/a')               -- Fallback to ERP data
    END                       AS gender,
    ca.bdate                  AS birthdate,
    ci.cst_create_date        AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key for dimension table
    pn.prd_id         AS product_id, -- Product identifiers
    pn.prd_key        AS product_number,
    pn.prd_nm         AS product_name,
    pn.cat_id         AS category_id, -- Category information
    pc.cat            AS category,
    pc.subcat         AS subcategory,
    pc.maintenance    AS maintenance,
    pn.prd_cost       AS cost,-- Product details
    pn.prd_line       AS product_line,
    pn.prd_start_dt   AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;  -- Only include active products
GO
-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
-- Purpose: Create a fact table that connects to dimensions via surrogate keys
-- Best Practice: Uses dimension surrogate keys instead of natural keys for:
--   - Consistent joins
--   - Slowly Changing Dimension (SCD) support
--   - Improved query performance
-- ======================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num    AS order_number,
    pr.product_key    AS product_key,         -- Dimension surrogate keys
    cu.customer_key   AS customer_key,
    sd.sls_order_dt   AS order_date,
    sd.sls_ship_dt    AS shipping_date,
    sd.sls_due_dt     AS due_date,
    sd.sls_sales      AS sales_amount,
    sd.sls_quantity   AS quantity,
    sd.sls_price      AS unit_price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO
