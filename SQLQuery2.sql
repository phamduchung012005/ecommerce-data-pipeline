IF OBJECT_ID('fact_order_items', 'U') IS NOT NULL DROP TABLE fact_order_items;

SELECT 
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    i.price,
    i.freight_value
INTO fact_order_items
FROM stg_order_items i
JOIN stg_orders o ON i.order_id = o.order_id;

PRINT 'Da tao thanh cong bang fact_order_items!';