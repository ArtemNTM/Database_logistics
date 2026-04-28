-- Проверка на совпадение total_fine/bonus и данных в finances
SELECT c.courier_id, c.total_fine, COALESCE(w.fine, 0), c.total_bonus, COALESCE(ww.bonus, 0) FROM logistics.couriers c
LEFT JOIN(
	SELECT f.courier_id, SUM(amount) AS fine
	FROM logistics.finances f
	WHERE f.operation_type = 'fine'
	GROUP BY f.courier_id
) AS w ON c.courier_id = w.courier_id
LEFT JOIN(
	SELECT f.courier_id, SUM(amount) AS bonus
	FROM logistics.finances f
	WHERE f.operation_type = 'bonus'
	GROUP BY f.courier_id
) AS ww ON c.courier_id = ww.courier_id
WHERE c.total_fine <> COALESCE(w.fine, 0) or  c.total_bonus <> COALESCE(ww.bonus, 0)


-- Курьеры и их транспорт
SELECT first_name, last_name, t.transport_name, t.transport_type  FROM logistics.couriers c
LEFT JOIN logistics.transport t
ON c.transport_id = t.transport_id

-- Топ 5 курьеров с самой большой разностью суммарно назначенных бонусов и штрафов
SELECT first_name, last_name, total_bonus - total_fine AS diff
FROM logistics.Couriers
ORDER BY diff DESC
LIMIT 5

-- Курьеры с наибольшим числом выполненных заказов
SELECT c.first_name, c.last_name, COUNT(*) AS cnt
FROM logistics.Delivery_logs l LEFT JOIN logistics.Couriers c
ON l.courier_id = c.courier_id
GROUP BY l.courier_id, c.first_name, c.last_name
ORDER BY cnt DESC
LIMIT 5

-- Самые заполненные склады
SELECT warehouse_id, city, street, building, capacity - current_load AS free_space
FROM logistics.Warehouse
ORDER BY free_space
LIMIT 5

-- Топ 5 клиентов по числу операций (отправки + получения)
SELECT client_id, first_name, last_name, receiv_num + send_num AS total_cnt
FROM logistics.Clients c
LEFT JOIN (
	SELECT o.sender_id, COUNT(*) AS send_num
	FROM logistics.Orders o
	GROUP BY o.sender_id
) AS send ON c.client_id = send.sender_id
LEFT JOIN (
	SELECT o.receiver_id, COUNT(*) AS receiv_num
	FROM logistics.Orders o
	GROUP BY o.receiver_id
) AS receive ON c.client_id = receive.receiver_id
ORDER BY total_cnt DESC
LIMIT 5

-- Топ курьеров с наибольшим числом опозданий
SELECT c.first_name, c.last_name, COUNT(l.log_id) AS late_deliveries
FROM logistics.Delivery_logs l
JOIN logistics.Couriers c ON l.courier_id = c.courier_id
WHERE l.actual_dt > l.plan_dt AND l.stage_status = 'completed'
GROUP BY c.courier_id, c.first_name, c.last_name
ORDER BY late_deliveries DESC
LIMIT 5;

-- Товары с наибольшей доходностью для компании
SELECT product_type, COUNT(order_id) AS orders_count, 
       SUM(delivery_cost) AS total_income
FROM logistics.Orders
WHERE status = 'delivered'
GROUP BY product_type
ORDER BY total_income DESC;

-- Какой вид транспорта наиболее популярен в доставке
SELECT t.transport_type, COUNT(l.log_id) AS usage_count
FROM logistics.Delivery_logs l
JOIN logistics.Transport t ON l.transport_id = t.transport_id
GROUP BY t.transport_type
ORDER BY usage_count DESC;

-- Статистика по всем заказам (какие доставили, какие отменились, какие в процессе)
SELECT status, COUNT(order_id) AS amount_of_orders
FROM logistics.Orders
GROUP BY status
ORDER BY amount_of_orders DESC;

