-- Xóa bảng dim_customers cũ nếu đã tồn tại để tránh lỗi khi chạy lại nhiều lần
IF OBJECT_ID('dim_customers', 'U') IS NOT NULL DROP TABLE dim_customers;

-- Lấy dữ liệu từ staging và tự động tạo thành bảng mới
SELECT 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
INTO dim_customers
FROM stg_customers;

PRINT 'Da tao thanh cong bang dim_customers!';