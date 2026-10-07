IF OBJECT_ID('dim_date', 'U') IS NOT NULL DROP TABLE dim_date;

-- Dùng hàm DISTINCT để lấy ra danh sách các ngày duy nhất (không trùng lặp)
-- và bóc tách các thành phần thời gian bằng các hàm có sẵn của SQL Server
SELECT DISTINCT 
    CAST(order_purchase_timestamp AS DATE) AS date_key,
    YEAR(order_purchase_timestamp) AS year,
    MONTH(order_purchase_timestamp) AS month,
    DAY(order_purchase_timestamp) AS day,
    DATEPART(QUARTER, order_purchase_timestamp) AS quarter,
    DATENAME(WEEKDAY, order_purchase_timestamp) AS weekday
INTO dim_date
FROM fact_order_items
WHERE order_purchase_timestamp IS NOT NULL;

PRINT 'Da tao thanh cong bang dim_date!';