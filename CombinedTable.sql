USE MetroDW;

CREATE TABLE star_schema_transactions (
    Order_ID VARCHAR(50) PRIMARY KEY,
    Order_Date DATE,
    ProductID VARCHAR(50),
    Quantity_Ordered INT,
    customer_id VARCHAR(50),
    time_id VARCHAR(50),
    customer_name VARCHAR(100),
    gender VARCHAR(20),
    productName VARCHAR(100),
    productPrice DECIMAL(10,2),
    supplierName VARCHAR(100),
    storeID VARCHAR(50),
    storeName VARCHAR(100),
    supplierID VARCHAR(50),
    TotalSales DECIMAL(10,2)
);

-- Indexes for performance optimization
CREATE INDEX idx_customer_id ON star_schema_transactions(customer_id);
CREATE INDEX idx_product_id ON star_schema_transactions(ProductID);
CREATE INDEX idx_time_id ON star_schema_transactions(time_id);
CREATE INDEX idx_order_date ON star_schema_transactions(Order_Date);