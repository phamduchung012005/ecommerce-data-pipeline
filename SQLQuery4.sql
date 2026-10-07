IF OBJECT_ID('dim_products', 'U') IS NOT NULL DROP TABLE dim_products;

SELECT 
    product_id,
    product_category_name,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
INTO dim_products
FROM stg_products;

PRINT 'Da tao thanh cong bang dim_products!';