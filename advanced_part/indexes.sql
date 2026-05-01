# Оптимизация соединений с delievery_logs
CREATE INDEX idx_delivery_logs_order_id ON logistics.delivery_logs (order_id);

# Поиск по активным заказам
CREATE INDEX idx_orders_active_status ON logistics.orders (status)
WHERE status IN ('created', 'in_progress');

# Оптимизация соединения с курьерами и поиска конкретной операции
CREATE INDEX idx_courier_finances_history ON logistics.finances (courier_id, created_time);

CREATE INDEX idx_orders_sender_id ON logistics.orders (sender_id);

CREATE INDEX idx_orders_receiver_id ON logistics.orders (receiver_id);

CREATE INDEX idx_clients_name_search ON logistics.clients (last_name, first_name, birth_date);

CREATE INDEX idx_couriers_name_search ON logistics.couriers (last_name, first_name, birth_date);