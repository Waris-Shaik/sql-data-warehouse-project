-- Check and drop the 'bronze.crm_cust_info' table if it exists
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

-- Create the 'bronze.crm_cust_info' table
CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);

GO

-- Check and drop the 'bronze.crm_prd_info' table if it exists
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

-- Create the 'bronze.crm_prd_info' table
CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);
GO

-- Check and drop the 'bronze.crm_sales_details' table if it exists
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

-- Create the 'bronze.crm_sales_details' table
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO

-- Check and drop the 'bronze.erp_loc_a101' table if it exists
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

-- Create the 'bronze.erp_loc_a101' table
CREATE TABLE bronze.erp_loc_a101 (
    cid   NVARCHAR(50),
    cntry NVARCHAR(50)
);
GO

-- Check and drop the 'bronze.erp_cust_az12' table if it exists
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

-- Create the 'bronze.erp_cust_az12' table
CREATE TABLE bronze.erp_cust_az12 (
    cid   NVARCHAR(50),
    bdate DATE,
    gen   NVARCHAR(50)
);
GO

-- Check and drop the 'bronze.erp_px_cat_g1v2' table if it exists
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

-- Create the 'bronze.erp_px_cat_g1v2' table
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO

-- Create or alter the procedure to load data into the bronze layer
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @batch_start_time DATETIME;
	DECLARE @batch_end_time DATETIME;
    DECLARE @start_time DATETIME;
    DECLARE @end_time DATETIME;
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ErrorLine INT;

    BEGIN TRANSACTION;

    BEGIN TRY
		SET @batch_start_time = GETDATE();
        PRINT '=======================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '=======================================================';

        PRINT '-------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '-------------------------------------------------------';
        
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT '>> Inserting Data Into Table: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\hp\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------'
        
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT '>> Inserting Data Into Table: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\hp\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------'
        
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '>> Inserting Data Into Table: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\hp\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------'

        PRINT '-------------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '-------------------------------------------------------';
        
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '>> Inserting Data Into Table: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\hp\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------'
        
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT '>> Inserting Data Into Table: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\hp\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------'
        
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT '>> Inserting Data Into Table: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\hp\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------'
        
		SET @batch_end_time = GETDATE();
        PRINT '-------------------------------------------------------';
        PRINT 'Bronze Layer Load Completed Successfully in ' + CAST (DATEDIFF(MINUTE, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' minutes ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=======================================================';
        
        COMMIT TRANSACTION;
		
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();
        SET @ErrorLine = ERROR_LINE();
        
        PRINT '-------------------------------------------------------';
        PRINT 'Error Encountered During Bronze Load';
        PRINT 'Error on Line: ' + CAST(@ErrorLine AS NVARCHAR(10));
        PRINT 'Error Message: ' + @ErrorMessage;
        PRINT 'Rolling Back Transaction...';
        PRINT '=======================================================';
        
        ROLLBACK TRANSACTION;
    END CATCH;
END;



-- Query to select all data from the newly created tables
SELECT * FROM bronze.crm_cust_info;
SELECT * FROM bronze.crm_prd_info;
SELECT * FROM bronze.crm_sales_details;
SELECT * FROM bronze.erp_loc_a101;
SELECT * FROM bronze.erp_cust_az12;
SELECT * FROM bronze.erp_px_cat_g1v2;

-- Check all tables in the database
SELECT * FROM sys.tables;

-- Execute the load process
EXEC bronze.load_bronze;

