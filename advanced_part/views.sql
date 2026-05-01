# 1 Worst couriers
CREATE MATERIALIZED VIEW mv_couriers_risk_zone AS
SELECT 
    c.courier_id,
    c.first_name,
    c.last_name,
    c.rating,
    c.phone,
    c.total_fine,
    COUNT(dl.log_id) AS delayed_deliveries_cnt
FROM logistics.couriers c
LEFT JOIN logistics.delivery_logs dl ON c.courier_id = dl.courier_id AND dl.actual_dt > dl.plan_dt
GROUP BY c.courier_id, c.first_name, c.last_name, c.rating, c.phone, c.total_fine
ORDER BY c.rating, delayed_deliveries_cnt DESC
LIMIT 10;

SELECT * FROM mv_couriers_risk_zone

# 2
CREATE OR REPLACE VIEW logistics.v_lost_orders_investigation AS
WITH LastLog AS (
    SELECT DISTINCT ON (order_id) 
        order_id, 
        courier_id, 
        stage_type
    FROM logistics.delivery_logs
    ORDER BY order_id, receive_dt DESC
)
SELECT 
    o.order_id,
    o.volume,
    o.product_type,
    o.sender_id,
    o.receiver_id,
    ll.stage_type AS last_known_stage,
    ll.courier_id AS last_courier_id
FROM logistics.orders o
LEFT JOIN LastLog ll ON o.order_id = ll.order_id
WHERE o.status = 'lost';

SELECT * FROM logistics.v_lost_orders_investigation;