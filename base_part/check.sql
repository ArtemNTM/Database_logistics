SELECT Orders.order_id
FROM logistics.Orders
LEFT JOIN logistics.Clients ON Orders.sender_id = Clients.client_id
WHERE Clients.client_id IS NULL;

SELECT Orders.order_id
FROM logistics.Orders
LEFT JOIN logistics.Clients ON Orders.receiver_id = Clients.client_id
WHERE Clients.client_id IS NULL;

SELECT Delivery_logs.log_id
FROM logistics.Delivery_logs
LEFT JOIN logistics.Orders ON Delivery_logs.order_id = Orders.order_id
WHERE Orders.order_id IS NULL;

SELECT Delivery_logs.log_id
FROM logistics.Delivery_logs
LEFT JOIN logistics.Couriers ON Delivery_logs.courier_id = Couriers.courier_id
WHERE Couriers.courier_id IS NULL;

SELECT Delivery_logs.log_id, Delivery_logs.courier_id, Delivery_logs.transport_id, Couriers.transport_id
FROM logistics.Delivery_logs
LEFT JOIN logistics.Couriers ON Delivery_logs.courier_id = Couriers.courier_id
WHERE Delivery_logs.transport_id <> Couriers.transport_id;

SELECT Delivery_logs.log_id
FROM logistics.Delivery_logs
LEFT JOIN logistics.Transport ON Delivery_logs.transport_id = Transport.transport_id
WHERE Transport.transport_id IS NULL;

SELECT Delivery_logs.log_id
FROM logistics.Delivery_logs
LEFT JOIN logistics.Warehouse ON Delivery_logs.src_wh_id = Warehouse.warehouse_id
WHERE Delivery_logs.src_wh_id IS NOT NULL AND Warehouse.warehouse_id IS NULL;

SELECT Delivery_logs.log_id
FROM logistics.Delivery_logs
LEFT JOIN logistics.Warehouse ON Delivery_logs.dst_wh_id = Warehouse.warehouse_id
WHERE Delivery_logs.dst_wh_id IS NOT NULL AND Warehouse.warehouse_id IS NULL;

SELECT Finances.operation_id
FROM logistics.Finances
LEFT JOIN logistics.Couriers ON Finances.courier_id = Couriers.courier_id
WHERE Couriers.courier_id IS NULL;

SELECT Finances.operation_id
FROM logistics.Finances
LEFT JOIN logistics.Orders ON Finances.order_id = Orders.order_id
WHERE Orders.order_id IS NULL;

SELECT Finances.operation_id
FROM logistics.Finances
LEFT JOIN logistics.Delivery_logs ON Finances.log_id = Delivery_logs.log_id
WHERE Delivery_logs.log_id IS NULL;

