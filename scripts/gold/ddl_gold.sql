-- Drop the existing view if it exists before creating a new one
IF OBJECT_ID('gold.dim_customers') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

-- Create the gold.dim_customers view
CREATE VIEW gold.dim_customers AS
SELECT
    -- Generate a unique customer key using ROW_NUMBER
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,                -- Unique Customer ID
    ci.cst_key AS customer_number,           -- Customer Number from CRM
    ci.cst_firstname AS first_name,          -- Customer's First Name
    ci.cst_lastname AS last_name,            -- Customer's Last Name
    la.cntry AS country,                     -- Country Information
    ci.cst_marital_status AS marital_status, -- Marital Status
    
    -- Determine Gender: Prefer CRM data, fallback to ERP data if unavailable
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE ISNULL(ca.gen, 'n/a')
    END AS gender,
    
    ca.bdate AS birthdate,                   -- Birthdate from ERP
    ci.cst_create_date AS create_date        -- Customer Account Creation Date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ca.cid = ci.cst_key  -- Join with ERP customer data
LEFT JOIN silver.erp_loc_a101 la ON la.cid = ci.cst_key; -- Join with location data
GO

-- Drop the existing view if it exists before creating a new one
IF OBJECT_ID('gold.dim_products') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

-- Create the gold.dim_products view
CREATE VIEW gold.dim_products AS
SELECT
    -- Generate a unique product key using ROW_NUMBER
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id AS product_id,           -- Unique Product ID
    pn.prd_key AS product_number,      -- Product Number from CRM
    pn.prd_nm AS product_name,         -- Product Name
    pn.cat_id AS category_id,          -- Category ID
    pc.cat AS category,                -- Category Name
    pc.subcat AS subcategory,          -- Subcategory Name
    pc.maintenance,                    -- Maintenance Information
    pn.prd_cost AS cost,               -- Product Cost
    pn.prd_line AS product_line,       -- Product Line Classification
    pn.prd_start_dt AS start_date      -- Start Date of Product Availability
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pc.id = pn.cat_id  -- Join with Category Data
WHERE pn.prd_end_dt IS NULL; -- Exclude historical/inactive products
GO

-- Drop the existing view if it exists before creating a new one
IF OBJECT_ID('gold.fact_sales') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

-- Create the gold.fact_sales view
CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,   -- Sales Order Number
    pr.product_key,                   -- Product Key (FK from dim_products)
    cu.customer_key,                  -- Customer Key (FK from dim_customers)
    sd.sls_order_dt AS order_date,    -- Order Date
    sd.sls_ship_dt AS shipping_date,  -- Shipping Date
    sd.sls_due_dt AS due_date,        -- Due Date
    sd.sls_sales AS sales_amount,     -- Total Sales Amount
    sd.sls_quantity AS quantity,      -- Quantity Sold
    sd.sls_price AS price             -- Sale Price Per Unit
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr ON pr.product_number = sd.sls_prd_key  -- Join with Products
LEFT JOIN gold.dim_customers cu ON cu.customer_id = sd.sls_cust_id;   -- Join with Customers
GO
