CREATE DATABASE MetroDW;
USE MetroDW;

CREATE TABLE products (
    productID INT PRIMARY KEY,
    productName VARCHAR(250),
    productPrice DECIMAL(10, 2),
    supplierName VARCHAR(250),
    storeID INT,
    storeName VARCHAR(250),
    supplierID INT
);

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(250),
    gender VARCHAR(10)
);

CREATE TABLE products_staging (
    productID INT,
    productName VARCHAR(255),
    productPrice VARCHAR(255), -- Store raw price as VARCHAR
    supplierName VARCHAR(255),
    storeID INT,
    storeName VARCHAR(255),
    supplierID INT
);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 9.1\\Uploads\\products_data.csv'
INTO TABLE products_staging
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(productID, productName, productPrice, supplierName, storeID, storeName, supplierID);

-- Load "products_data.csv" into a table
INSERT INTO products (productID, productName, productPrice, supplierName, storeID, storeName, supplierID)
SELECT 
    productID,
    productName,
    CAST(REPLACE(productPrice, '$', '') AS DECIMAL(10, 2)), -- Remove '$' and cast to DECIMAL
    supplierName,
    storeID,
    storeName,
    supplierID
FROM 
    products_staging;


-- Load "customers_data.csv" into a table
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 9.1\\Uploads\\customers_data.csv'
INTO TABLE customers
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(customer_id, customer_name, gender);

DROP TABLE products_staging;

