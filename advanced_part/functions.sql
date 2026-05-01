# Сбор статистик 
# justify_interval - для аккуратной конвертации часов в дни и дней в месяцы
CREATE OR REPLACE FUNCTION logistics.get_orders_statistics_by_period (
  left_period_date DATE,
  right_period_date DATE
)
RETURNS TABLE(
  product_type VARCHAR(100),
  orders_count BIGINT, 
  delivered_num BIGINT, 
  cancelled_num BIGINT, 
  lost_num BIGINT,
  success_percent NUMERIC(5, 2),
  avg_delivery_time INTERVAL
)
LANGUAGE sql
STABLE
AS $$
  WITH order_time AS (
    SELECT DISTINCT ON (dl.order_id)
      dl.order_id,
      dl.actual_dt
    FROM logistics.delivery_logs dl
    WHERE dl.stage_status IN ('completed', 'cancelled', 'lost') AND dl.stage_type = 'delivery'
    ORDER BY dl.order_id, dl.receive_dt DESC
  )
  SELECT
    o.product_type,
    COUNT(*) AS orders_count,
    SUM(CASE o.status WHEN 'delivered' THEN 1 ELSE 0 END) AS delivered_num,
    SUM(CASE o.status WHEN 'cancelled' THEN 1 ELSE 0 END) AS cancelled_num,
    SUM(CASE o.status WHEN 'lost' THEN 1 ELSE 0 END) AS lost_num,
    ROUND((SUM(CASE o.status WHEN 'delivered' THEN 1 ELSE 0 END)  * 100.0), 2) / COUNT(*) AS success_percent,
    justify_interval(AVG(AGE(ot.actual_dt, o.order_date))) AS avg_delivery_time
  FROM logistics.orders o
  INNER JOIN order_time ot ON o.order_id = ot.order_id
  WHERE (left_period_date IS NULL OR o.order_date >= left_period_date) 
        AND (right_period_date IS NULL OR ot.actual_dt <= right_period_date) 
  GROUP BY product_type
$$;

SELECT * FROM logistics.get_orders_statistics_by_period(NULL, NULL);


# Сбор статистики по каждому курьеру
CREATE OR REPLACE FUNCTION logistics.get_courier_statistics_by_period (
  left_period_date DATE,
  right_period_date DATE
)
RETURNS TABLE(
  courier_id INT,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  total_deliveries BIGINT, 
  on_time_deliveries BIGINT, 
  late_deliveries BIGINT, 
  lost_deliveries BIGINT
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    c.courier_id,
    c.first_name,
    c.last_name,
    COUNT(*) AS total_deliveries,
    COUNT(*) FILTER (WHERE dl.stage_status = 'completed' AND dl.actual_dt <= dl.plan_dt) AS on_time_deliveries,
    COUNT(*) FILTER (WHERE dl.stage_status = 'completed' AND dl.actual_dt > dl.plan_dt) AS late_deliveries,
    COUNT(*) FILTER (WHERE dl.stage_status = 'lost') AS lost_deliveries
  FROM logistics.Couriers c
  INNER JOIN logistics.Delivery_logs dl ON c.courier_id = dl.courier_id
  WHERE dl.stage_type = 'delivery' 
    AND dl.stage_status IN ('completed', 'lost')
    AND (left_period_date IS NULL OR dl.actual_dt >= left_period_date) 
    AND (right_period_date IS NULL OR dl.actual_dt <= right_period_date)
  GROUP BY 
    c.courier_id, 
    c.first_name, 
    c.last_name;
$$;

SELECT * FROM logistics.get_courier_statistics_by_period(NULL, NULL);