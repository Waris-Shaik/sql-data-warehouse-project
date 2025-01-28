-- Switch to the 'master' database to perform database-level operations
USE master;
GO

-- Check if the database 'DataWareHouse' already exists
-- If it does, drop the existing database to start fresh
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWareHouse')
BEGIN
    DROP DATABASE DataWareHouse;
END;
GO

-- Create a new database named 'DataWareHouse'
-- This will serve as the central repository for our data warehousing project
CREATE DATABASE DataWareHouse;
GO

-- Switch to the newly created 'DataWareHouse' database to perform further operations
USE DataWareHouse;
GO

-- Create the 'bronze' schema
-- The bronze schema typically stores raw, unprocessed data ingested from various sources
CREATE SCHEMA bronze;
GO

-- Create the 'silver' schema
-- The silver schema is used to store cleansed and transformed data
-- This is the intermediate layer where data is structured and enriched
CREATE SCHEMA silver;
GO

-- Create the 'gold' schema
-- The gold schema contains aggregated, business-ready data
-- This data is optimized for analytics and reporting purposes
CREATE SCHEMA gold;
GO
