create database retail_analytics;

use retail_analytics;

CREATE TABLE dim_category (
    category_id VARCHAR(20) PRIMARY KEY,
    category_name VARCHAR(100)
);


CREATE TABLE dim_products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(150),
    category_id VARCHAR(20),
    launch_date DATE,
    price DECIMAL(10,2),
    FOREIGN KEY (category_id) REFERENCES dim_category(category_id)
);


CREATE TABLE dim_stores (
    store_id VARCHAR(20) PRIMARY KEY,
    store_name VARCHAR(150),
    city VARCHAR(100),
    country VARCHAR(100)
);


CREATE TABLE dim_date (
    date DATE PRIMARY KEY,
    year INT,
    month INT,
    month_name VARCHAR(20),
    quarter VARCHAR(10),
    weekday VARCHAR(20)
);


CREATE TABLE fact_sales (
    sale_id VARCHAR(50) PRIMARY KEY,
    sale_date DATE,
    product_id VARCHAR(20),
    store_id VARCHAR(20),
    quantity INT,
    revenue DECIMAL(12,2),
    year INT,
    month INT,
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (store_id) REFERENCES dim_stores(store_id),
    FOREIGN KEY (sale_date) REFERENCES dim_date(date)
);


CREATE TABLE fact_warranty (
    claim_id VARCHAR(50) PRIMARY KEY,
    claim_date DATE,
    sale_id VARCHAR(50),
    repair_status VARCHAR(50),
    is_completed TINYINT,
    year INT,
    month INT,
    FOREIGN KEY (sale_id) REFERENCES fact_sales(sale_id),
    FOREIGN KEY (claim_date) REFERENCES dim_date(date)
);

show tables;

SELECT COUNT(*) FROM dim_category;
SELECT COUNT(*) FROM dim_products;
SELECT COUNT(*) FROM dim_stores;
SELECT COUNT(*) FROM dim_date;


SELECT COUNT(*) FROM fact_sales;

select count(*) from fact_warranty;

SET FOREIGN_KEY_CHECKS = 0;

select count(*) from fact_sales;


-- Quick validation

SELECT * FROM fact_sales LIMIT 5;
SELECT * FROM fact_warranty LIMIT 5;

-- Total Revenue

SELECT SUM(revenue) AS total_revenue
FROM fact_sales;

-- Monthly sales trend

SELECT year, month, SUM(revenue) AS monthly_revenue
FROM fact_sales
GROUP BY year, month
ORDER BY year, month;

-- Top 5 products by Revenue

SELECT p.product_name, SUM(f.revenue) AS revenue
FROM fact_sales f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 5;

-- Store-wise performance

SELECT s.store_name, SUM(f.revenue) AS revenue
FROM fact_sales f
JOIN dim_stores s ON f.store_id = s.store_id
GROUP BY s.store_name
ORDER BY revenue DESC;

-- Warranty Rate
 
SELECT 
  COUNT(w.claim_id) * 100.0 / COUNT(DISTINCT f.sale_id) AS warranty_rate_percent
FROM fact_sales f
LEFT JOIN fact_warranty w ON f.sale_id = w.sale_id;










