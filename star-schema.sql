

DROP DATABASE IF EXISTS Wal_DW;
CREATE DATABASE Wal_DW;
USE Wal_DW;

-- =======================
-- DIMENSION TABLES
-- =======================

-- ---- DimCustomer ----
CREATE TABLE DimCustomer (
    Customer_ID INT PRIMARY KEY,
    Gender VARCHAR(10),
    Age VARCHAR(10),
    Occupation INT,
    City_Category VARCHAR(10),
    Stay_In_Current_City_Years INT,
    Marital_Status INT
);

-- ---- DimProduct ----
CREATE TABLE DimProduct (
    Product_ID VARCHAR(20) PRIMARY KEY,
    Product_Category VARCHAR(50),
    Price DECIMAL(10,2),
    Store_ID INT,
    Supplier_ID INT
);

-- ---- DimStore ----
CREATE TABLE DimStore (
    Store_ID INT PRIMARY KEY,
    Store_Name VARCHAR(100),
    City_Category VARCHAR(10)
);

-- ---- DimSupplier ----
CREATE TABLE DimSupplier (
    Supplier_ID INT PRIMARY KEY,
    Supplier_Name VARCHAR(100)
);

-- ---- DimTime ----
CREATE TABLE DimTime (
    Time_ID INT AUTO_INCREMENT PRIMARY KEY,
    Date DATE,
    Day INT,
    Month INT,
    Quarter INT,
    Year INT,
    Weekday_Name VARCHAR(15),
    Is_Weekend BOOLEAN
);

-- =======================
-- FACT TABLE
-- =======================

CREATE TABLE FactSales (
    Order_ID INT PRIMARY KEY,
    Customer_ID INT,
    Product_ID VARCHAR(20),
    Store_ID INT,
    Supplier_ID INT,
    Time_ID INT,
    Quantity INT,
    Revenue DECIMAL(12,2),

    FOREIGN KEY (Customer_ID) REFERENCES DimCustomer(Customer_ID),
    FOREIGN KEY (Product_ID) REFERENCES DimProduct(Product_ID),
    FOREIGN KEY (Store_ID) REFERENCES DimStore(Store_ID),
    FOREIGN KEY (Supplier_ID) REFERENCES DimSupplier(Supplier_ID),
    FOREIGN KEY (Time_ID) REFERENCES DimTime(Time_ID)
);

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

