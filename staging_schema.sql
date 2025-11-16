-- This table will temporarily hold the joined and transformed data
-- before it's loaded into the final star schema.
-- Please run this SQL command on your 'wal_DW' database to create the necessary table.

CREATE TABLE IF NOT EXISTS StagingSales (
    Staging_ID INT AUTO_INCREMENT PRIMARY KEY,
    Order_ID INT NOT NULL,
    `Date` DATETIME NOT NULL,
    Quantity INT,
    Revenue DECIMAL(10, 2),

    -- Customer Attributes
    Customer_ID INT NOT NULL,
    Gender CHAR(1),
    Age VARCHAR(20),
    Occupation INT,
    City_Category CHAR(1),
    Stay_In_Current_City_Years VARCHAR(10),
    Marital_Status INT,

    -- Product Attributes
    Product_ID VARCHAR(255) NOT NULL,
    Product_Category VARCHAR(255),
    Price DECIMAL(10, 2),

    -- Store Attributes
    Store_ID INT,
    Store_Name VARCHAR(255),

    -- Supplier Attributes
    Supplier_ID INT,
    Supplier_Name VARCHAR(255)
);
