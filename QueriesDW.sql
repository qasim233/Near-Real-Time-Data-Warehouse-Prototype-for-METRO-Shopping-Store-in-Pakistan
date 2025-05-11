USE star_schema_transactions;

-- Query 1
-- Specify the year for filtering
SET @specified_year = 2019;

SELECT 
    dt.month AS sales_month,
    CASE 
        WHEN DAYOFWEEK(dt.order_date) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS sales_day_type,
    dp.productName AS product_name,
    SUM(fs.total_sales) AS total_revenue
FROM 
    fact_sales fs
JOIN 
    dim_time dt ON fs.order_id = dt.order_id
JOIN 
    dim_product dp ON fs.productID = dp.productID
WHERE 
    dt.year = @specified_year
GROUP BY 
    dt.month, sales_day_type, dp.productName
ORDER BY 
    dt.month, sales_day_type, total_revenue DESC
LIMIT 5;

-- Query 2
-- Calculate the revenue growth rate for each store quarterly in 2017
WITH QuarterlyRevenue AS (
    SELECT 
        ds.storeID,
        ds.storeName,
        QUARTER(dt.order_date) AS quarter,
        SUM(fs.total_sales) AS total_revenue
    FROM 
        fact_sales fs
    JOIN 
        dim_time dt ON fs.order_id = dt.order_id
    JOIN 
        dim_store ds ON fs.storeID = ds.storeID
    WHERE 
        dt.year = 2019
    GROUP BY 
        ds.storeID, ds.storeName, QUARTER(dt.order_date)
),
QuarterlyGrowth AS (
    SELECT 
        qr.storeID,
        qr.storeName,
        qr.quarter,
        qr.total_revenue AS current_revenue,
        LAG(qr.total_revenue) OVER (PARTITION BY qr.storeID ORDER BY qr.quarter) AS previous_revenue,
        CASE 
            WHEN LAG(qr.total_revenue) OVER (PARTITION BY qr.storeID ORDER BY qr.quarter) IS NOT NULL THEN 
                ROUND(((qr.total_revenue - LAG(qr.total_revenue) OVER (PARTITION BY qr.storeID ORDER BY qr.quarter)) 
                / LAG(qr.total_revenue) OVER (PARTITION BY qr.storeID ORDER BY qr.quarter)) * 100, 2)
            ELSE NULL
        END AS growth_rate
    FROM 
        QuarterlyRevenue qr
)
SELECT 
    storeID,
    storeName,
    quarter,
    current_revenue,
    previous_revenue,
    growth_rate
FROM 
    QuarterlyGrowth
ORDER BY 
    storeID, quarter;

-- Query 3
SELECT 
    ds.storeID,
    ds.storeName,
    dsu.supplierID,
    dsu.supplierName,
    dp.productName,
    SUM(fs.total_sales) AS total_sales
FROM 
    fact_sales fs
JOIN 
    dim_store ds ON fs.storeID = ds.storeID
JOIN 
    dim_supplier dsu ON fs.supplierID = dsu.supplierID
JOIN 
    dim_product dp ON fs.productID = dp.productID
GROUP BY 
    ds.storeID, ds.storeName, dsu.supplierID, dsu.supplierName, dp.productName
ORDER BY 
    ds.storeID, dsu.supplierID, dp.productName;

-- Query 4
WITH SeasonalSales AS (
    SELECT 
        dp.productID,
        dp.productName,
        CASE 
            WHEN MONTH(dt.order_date) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(dt.order_date) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(dt.order_date) IN (9, 10, 11) THEN 'Fall'
            WHEN MONTH(dt.order_date) IN (12, 1, 2) THEN 'Winter'
        END AS season,
        SUM(fs.total_sales) AS total_sales
    FROM 
        fact_sales fs
    JOIN 
        dim_time dt ON fs.order_id = dt.order_id
    JOIN 
        dim_product dp ON fs.productID = dp.productID
    GROUP BY 
        dp.productID, dp.productName, season
)
SELECT 
    productID,
    productName,
    season,
    total_sales
FROM 
    SeasonalSales
ORDER BY 
    productName, 
    FIELD(season, 'Spring', 'Summer', 'Fall', 'Winter');
    
-- Query 5
WITH MonthlyRevenue AS (
    SELECT 
        ds.storeID,
        ds.storeName,
        dsu.supplierID,
        dsu.supplierName,
        dt.year,
        dt.month,
        SUM(fs.total_sales) AS total_revenue
    FROM 
        fact_sales fs
    JOIN 
        dim_store ds ON fs.storeID = ds.storeID
    JOIN 
        dim_supplier dsu ON fs.supplierID = dsu.supplierID
    JOIN 
        dim_time dt ON fs.order_id = dt.order_id
    GROUP BY 
        ds.storeID, ds.storeName, dsu.supplierID, dsu.supplierName, dt.year, dt.month
),
MonthlyVolatility AS (
    SELECT 
        mr.storeID,
        mr.storeName,
        mr.supplierID,
        mr.supplierName,
        mr.year,
        mr.month,
        mr.total_revenue AS current_revenue,
        LAG(mr.total_revenue) OVER (
            PARTITION BY mr.storeID, mr.supplierID 
            ORDER BY mr.year, mr.month
        ) AS previous_revenue,
        CASE 
            WHEN LAG(mr.total_revenue) OVER (
                PARTITION BY mr.storeID, mr.supplierID 
                ORDER BY mr.year, mr.month
            ) IS NOT NULL THEN 
                ROUND(
                    ((mr.total_revenue - LAG(mr.total_revenue) OVER (
                        PARTITION BY mr.storeID, mr.supplierID 
                        ORDER BY mr.year, mr.month
                    )) / LAG(mr.total_revenue) OVER (
                        PARTITION BY mr.storeID, mr.supplierID 
                        ORDER BY mr.year, mr.month
                    )) * 100, 
                    2
                )
            ELSE NULL
        END AS revenue_volatility
    FROM 
        MonthlyRevenue mr
)
SELECT 
    storeID,
    storeName,
    supplierID,
    supplierName,
    year,
    month,
    current_revenue,
    previous_revenue,
    revenue_volatility
FROM 
    MonthlyVolatility
ORDER BY 
    storeID, supplierID, year, month;

-- Query 6
WITH ProductPairs AS (
    SELECT 
        fs1.productID AS product1_id,
        dp1.productName AS product1_name,
        fs2.productID AS product2_id,
        dp2.productName AS product2_name,
        COUNT(*) AS purchase_count
    FROM 
        fact_sales fs1
    JOIN 
        fact_sales fs2 ON fs1.order_id = fs2.order_id AND fs1.productID < fs2.productID
    JOIN 
        dim_product dp1 ON fs1.productID = dp1.productID
    JOIN 
        dim_product dp2 ON fs2.productID = dp2.productID
    GROUP BY 
        fs1.productID, dp1.productName, fs2.productID, dp2.productName
    ORDER BY 
        purchase_count DESC
)
SELECT 
    product1_id,
    product1_name,
    product2_id,
    product2_name,
    purchase_count
FROM 
    ProductPairs
LIMIT 5;

-- Query 7
SELECT 
    COALESCE(s.storeName, 'All Stores') AS Store,
    COALESCE(sup.supplierName, 'All Suppliers') AS Supplier,
    COALESCE(p.productName, 'All Products') AS Product,
    t.year AS Year,
    -- Aggregated measures
    SUM(f.total_sales) AS Total_Revenue,
    SUM(f.quantity_ordered) AS Total_Units_Sold,
    ROUND(SUM(f.total_sales) / SUM(f.quantity_ordered), 2) AS Avg_Price_Per_Unit,
    -- Calculate percentage of store total
    ROUND(SUM(f.total_sales) * 100.0 / 
        SUM(SUM(f.total_sales)) OVER (PARTITION BY s.storeName, t.year), 2) AS Pct_of_Store_Revenue
FROM fact_sales f
JOIN dim_store s ON f.storeID = s.storeID
JOIN dim_supplier sup ON f.supplierID = sup.supplierID
JOIN dim_product p ON f.productID = p.productID
JOIN dim_time t ON f.order_id = t.order_id
GROUP BY 
    s.storeName,
    sup.supplierName,
    p.productName,
    t.year
WITH ROLLUP
HAVING year IS NOT NULL  -- Exclude rollups across years
ORDER BY 
    CASE WHEN Store = 'All Stores' THEN 1 ELSE 0 END,
    Store,
    CASE WHEN Supplier = 'All Suppliers' THEN 1 ELSE 0 END,
    Supplier,
    CASE WHEN Product = 'All Products' THEN 1 ELSE 0 END,
    Product,
    Year;
    
-- Query 8
WITH half_year_sales AS (
    SELECT 
        p.productID,
        p.productName,
        p.productPrice,
        CASE 
            WHEN t.month <= 6 THEN 'H1'
            ELSE 'H2'
        END AS half_year,
        t.year,
        SUM(f.quantity_ordered) as units_sold,
        SUM(f.total_sales) as revenue
    FROM fact_sales f
    JOIN dim_product p ON f.productID = p.productID
    JOIN dim_time t ON f.order_id = t.order_id
    GROUP BY 
        p.productID,
        p.productName,
        p.productPrice,
        t.year,
        CASE 
            WHEN t.month <= 6 THEN 'H1'
            ELSE 'H2'
        END
)
SELECT 
    p.productID,
    p.productName,
    p.productPrice AS current_price,
    year,
    -- H1 Metrics
    MAX(CASE WHEN half_year = 'H1' THEN units_sold END) as H1_units_sold,
    MAX(CASE WHEN half_year = 'H1' THEN revenue END) as H1_revenue,
    ROUND(MAX(CASE WHEN half_year = 'H1' THEN revenue/units_sold END), 2) as H1_avg_price,
    -- H2 Metrics
    MAX(CASE WHEN half_year = 'H2' THEN units_sold END) as H2_units_sold,
    MAX(CASE WHEN half_year = 'H2' THEN revenue END) as H2_revenue,
    ROUND(MAX(CASE WHEN half_year = 'H2' THEN revenue/units_sold END), 2) as H2_avg_price,
    -- Full Year Totals
    SUM(units_sold) as yearly_units_sold,
    SUM(revenue) as yearly_revenue,
    ROUND(SUM(revenue)/SUM(units_sold), 2) as yearly_avg_price,
    -- Growth Calculations
    ROUND(((MAX(CASE WHEN half_year = 'H2' THEN units_sold END) - 
            MAX(CASE WHEN half_year = 'H1' THEN units_sold END)) * 100.0 / 
            MAX(CASE WHEN half_year = 'H1' THEN units_sold END)), 2) as units_growth_pct,
    ROUND(((MAX(CASE WHEN half_year = 'H2' THEN revenue END) - 
            MAX(CASE WHEN half_year = 'H1' THEN revenue END)) * 100.0 / 
            MAX(CASE WHEN half_year = 'H1' THEN revenue END)), 2) as revenue_growth_pct
FROM half_year_sales p
GROUP BY 
    p.productID,
    p.productName,
    p.productPrice,
    year
ORDER BY 
    year,
    yearly_revenue DESC,
    productName;
    
-- Query 9
WITH daily_stats AS (
    -- Calculate daily sales metrics
    SELECT 
        t.order_date,
        p.productID,
        p.productName,
        SUM(f.quantity_ordered) as daily_units,
        SUM(f.total_sales) as daily_revenue
    FROM fact_sales f
    JOIN dim_product p ON f.productID = p.productID
    JOIN dim_time t ON f.order_id = t.order_id
    GROUP BY t.order_date, p.productID, p.productName
),
product_averages AS (
    -- Calculate product-level averages and standard deviations
    SELECT 
        productID,
        productName,
        AVG(daily_units) as avg_daily_units,
        STDDEV(daily_units) as stddev_units,
        AVG(daily_revenue) as avg_daily_revenue,
        STDDEV(daily_revenue) as stddev_revenue,
        COUNT(*) as total_days_with_sales
    FROM daily_stats
    GROUP BY productID, productName
),
spike_analysis AS (
    -- Identify spikes and calculate deviation metrics
    SELECT 
        d.order_date,
        d.productID,
        d.productName,
        d.daily_units,
        d.daily_revenue,
        pa.avg_daily_units,
        pa.avg_daily_revenue,
        pa.stddev_units,
        pa.stddev_revenue,
        -- Calculate Z-scores
        ROUND((d.daily_units - pa.avg_daily_units) / NULLIF(pa.stddev_units, 0), 2) as units_z_score,
        ROUND((d.daily_revenue - pa.avg_daily_revenue) / NULLIF(pa.stddev_revenue, 0), 2) as revenue_z_score,
        -- Calculate percentage above average
        ROUND((d.daily_units - pa.avg_daily_units) * 100.0 / pa.avg_daily_units, 2) as units_pct_above_avg,
        ROUND((d.daily_revenue - pa.avg_daily_revenue) * 100.0 / pa.avg_daily_revenue, 2) as revenue_pct_above_avg
    FROM daily_stats d
    JOIN product_averages pa ON d.productID = pa.productID
)
SELECT 
    order_date,
    productName,
    daily_units,
    daily_revenue,
    ROUND(avg_daily_units, 2) as avg_daily_units,
    ROUND(avg_daily_revenue, 2) as avg_daily_revenue,
    units_z_score,
    revenue_z_score,
    units_pct_above_avg,
    revenue_pct_above_avg,
    CASE 
        WHEN units_z_score >= 3 AND revenue_z_score >= 3 THEN 'CRITICAL SPIKE'
        WHEN units_z_score >= 2 AND revenue_z_score >= 2 THEN 'HIGH SPIKE'
        WHEN units_z_score >= 1.5 OR revenue_z_score >= 1.5 THEN 'MODERATE SPIKE'
        ELSE 'NORMAL'
    END as spike_category,
    CASE 
        WHEN units_pct_above_avg > revenue_pct_above_avg + 10 THEN 'Possible Discount Event'
        WHEN revenue_pct_above_avg > units_pct_above_avg + 10 THEN 'Possible Premium Sales'
        ELSE 'Normal Price Range'
    END as price_pattern
FROM spike_analysis
WHERE units_z_score >= 1.5 OR revenue_z_score >= 1.5  -- Show only significant spikes
ORDER BY 
    revenue_z_score DESC,
    order_date;
    
-- Query 10
CREATE OR REPLACE VIEW STORE_QUARTERLY_SALES AS
WITH quarterly_data AS (
    SELECT 
        s.storeID,
        s.storeName,
        t.year,
        CASE 
            WHEN t.month BETWEEN 1 AND 3 THEN 1
            WHEN t.month BETWEEN 4 AND 6 THEN 2
            WHEN t.month BETWEEN 7 AND 9 THEN 3
            ELSE 4
        END AS quarter,
        SUM(f.total_sales) as quarterly_revenue,
        SUM(f.quantity_ordered) as units_sold,
        COUNT(DISTINCT f.customer_id) as unique_customers,
        COUNT(DISTINCT f.productID) as unique_products,
        COUNT(DISTINCT f.order_id) as total_transactions
    FROM fact_sales f
    JOIN dim_store s ON f.storeID = s.storeID
    JOIN dim_time t ON f.order_id = t.order_id
    GROUP BY 
        s.storeID,
        s.storeName,
        t.year,
        CASE 
            WHEN t.month BETWEEN 1 AND 3 THEN 1
            WHEN t.month BETWEEN 4 AND 6 THEN 2
            WHEN t.month BETWEEN 7 AND 9 THEN 3
            ELSE 4
        END
)
SELECT 
    storeID,
    storeName,
    year,
    quarter,
    quarterly_revenue,
    units_sold,
    unique_customers,
    unique_products,
    total_transactions,
    -- Performance Metrics
    ROUND(quarterly_revenue / NULLIF(units_sold, 0), 2) as avg_unit_price,
    ROUND(quarterly_revenue / NULLIF(unique_customers, 0), 2) as avg_customer_spend,
    ROUND(quarterly_revenue / NULLIF(total_transactions, 0), 2) as avg_transaction_value,
    ROUND(units_sold / NULLIF(total_transactions, 0), 2) as avg_units_per_transaction,
    -- Quarter-over-Quarter Growth
    ROUND((quarterly_revenue - LAG(quarterly_revenue) 
        OVER (PARTITION BY storeID ORDER BY year, quarter)) * 100.0 / 
        NULLIF(LAG(quarterly_revenue) 
        OVER (PARTITION BY storeID ORDER BY year, quarter), 0), 2) as revenue_growth_pct,
    -- Year-over-Year Growth
    ROUND((quarterly_revenue - LAG(quarterly_revenue, 4) 
        OVER (PARTITION BY storeID ORDER BY year, quarter)) * 100.0 / 
        NULLIF(LAG(quarterly_revenue, 4) 
        OVER (PARTITION BY storeID ORDER BY year, quarter), 0), 2) as yoy_growth_pct,
    -- Quarterly Rankings
    RANK() OVER (PARTITION BY year, quarter ORDER BY quarterly_revenue DESC) as revenue_rank,
    RANK() OVER (PARTITION BY year, quarter ORDER BY unique_customers DESC) as customer_rank,
    -- Running Totals
    SUM(quarterly_revenue) OVER (PARTITION BY storeID, year ORDER BY quarter) as ytd_revenue,
    SUM(units_sold) OVER (PARTITION BY storeID, year ORDER BY quarter) as ytd_units
FROM quarterly_data
ORDER BY 
    storeName,
    year DESC,
    quarter DESC;