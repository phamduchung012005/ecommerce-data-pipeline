IF OBJECT_ID('rfm_model', 'U') IS NOT NULL DROP TABLE rfm_model;

-- 1. Tìm ngày giao dịch cuối cùng trong hệ thống để làm mốc tính toán (tránh việc dùng ngày hiện tại khiến Recency bị quá lớn)
DECLARE @max_date DATE = (SELECT MAX(CAST(order_purchase_timestamp AS DATE)) FROM fact_order_items);

-- 2. Tính toán các chỉ số RFM
SELECT 
    c.customer_unique_id,
    DATEDIFF(DAY, MAX(CAST(f.order_purchase_timestamp AS DATE)), @max_date) AS recency,
    COUNT(DISTINCT f.order_id) AS frequency,
    SUM(f.price) AS monetary
INTO rfm_model
FROM fact_order_items f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.customer_unique_id;

PRINT 'Da tao thanh cong bang rfm_model!';