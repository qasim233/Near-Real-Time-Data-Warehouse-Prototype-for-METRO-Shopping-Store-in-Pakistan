DROP DATABASE IF EXISTS star_schema_transactions;
CREATE DATABASE star_schema_transactions;
USE star_schema_transactions;

-- Drop Dimension Tables if they exist
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_supplier;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_time;

-- Dimension Tables
CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    gender VARCHAR(10)
);

CREATE TABLE dim_product (
    productID INT PRIMARY KEY,
    productName VARCHAR(100),
    productPrice DECIMAL(10,2)
);

CREATE TABLE dim_supplier (
	supplierID INT PRIMARY KEY,
    supplierName VARCHAR(100)
);

CREATE TABLE dim_store (
	storeID INT PRIMARY KEY,
    storeName VARCHAR(100)
);

CREATE TABLE dim_time (
	order_id INT PRIMARY KEY,
    time_id INT NOT NULL,
    order_date DATE,
    year INT,
    month INT,
    day INT
);

-- Fact Table
CREATE TABLE fact_sales (
    sales_sk INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    customer_id INT,
    productID INT,
    supplierID INT,
    storeID INT,
    quantity_ordered INT,
    total_sales DECIMAL(10,2),
    
    FOREIGN KEY (order_id) REFERENCES dim_time(order_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (productID) REFERENCES dim_product(productID),
    FOREIGN KEY (supplierID) REFERENCES dim_supplier(supplierID),
    FOREIGN KEY (storeID) REFERENCES dim_store(storeID)
);

USE star_schema_transactions;

INSERT INTO dim_customer (customer_id, customer_name, gender)
SELECT DISTINCT customer_id, customer_name, gender
FROM MetroDW.star_schema_transactions;

INSERT INTO dim_product (productID, productName, productPrice)
SELECT DISTINCT ProductID, productName, productPrice
FROM MetroDW.star_schema_transactions;

INSERT INTO dim_supplier (supplierID, supplierName)
SELECT DISTINCT supplierID, supplierName
FROM MetroDW.star_schema_transactions;

INSERT INTO dim_store (storeID, storeName)
SELECT DISTINCT storeID, storeName
FROM MetroDW.star_schema_transactions;

INSERT INTO dim_time (order_id, time_id, order_date, year, month, day)
SELECT 
    order_id,
    time_id,
    Order_Date,
    YEAR(Order_Date) AS year,
    MONTH(Order_Date) AS month,
    DAY(Order_Date) AS day
FROM MetroDW.star_schema_transactions
GROUP BY order_id, time_id, Order_Date;


INSERT INTO fact_sales (order_id, customer_id, productID, supplierID, storeID, quantity_ordered, total_sales)
SELECT 
    t.order_id,
    t.customer_id,
    t.ProductID,
    t.supplierID,
    t.storeID,
    t.Quantity_Ordered,
    t.TotalSales
FROM MetroDW.star_schema_transactions t;

