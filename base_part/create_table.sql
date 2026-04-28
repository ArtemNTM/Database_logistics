CREATE SCHEMA IF NOT EXISTS logistics;

CREATE TABLE IF NOT EXISTS logistics.CLients(
	client_id SERIAL PRIMARY KEY,
	first_name VARCHAR(100) NOT NULL,
	last_name VARCHAR(100) NOT NULL,
	birth_date DATE NOT NULL CHECK(birth_date <= CURRENT_DATE),
	city VARCHAR(100) NOT NULL,
	phone VARCHAR(20) UNIQUE NOT NULL CHECK(phone ~ '^\+7[0-9]{10}$'),
	email VARCHAR(255) UNIQUE CHECK(email LIKE '%@%')
);

CREATE TABLE IF NOT EXISTS logistics.Warehouse (
    warehouse_id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    street VARCHAR(255) NOT NULL,
    building VARCHAR(10) NOT NULL,
    capacity DECIMAL(10,2) NOT NULL CHECK(capacity > 0),
    current_load DECIMAL(10,2) DEFAULT 0 CHECK(current_load BETWEEN 0 AND capacity)
);

CREATE TABLE IF NOT EXISTS logistics.Transport (
    transport_id SERIAL PRIMARY KEY,
    transport_name VARCHAR(100) NOT NULL UNIQUE,
    transport_type VARCHAR(50) NOT NULL CHECK(transport_type IN ('ship', 'plane', 'truck', 'van', 'car', 'scooter', 'bicycle')),
    payload_capacity NUMERIC NOT NULL CHECK(payload_capacity > 0),
    cost_per_km DECIMAL NOT NULL CHECK(cost_per_km >= 0),
    units_cnt INT NOT NULL CHECK(units_cnt >= 0)
);

CREATE TABLE IF NOT EXISTS logistics.Orders (
    order_id SERIAL PRIMARY KEY,
    sender_id INT NOT NULL REFERENCES logistics.Clients(client_id),
    receiver_id INT NOT NULL REFERENCES logistics.Clients(client_id),
    volume NUMERIC(10,2) NOT NULL CHECK(volume > 0),
    product_type VARCHAR(100) NOT NULL CHECK(product_type IN ('standard', 'fragile', 'perishable', 'oversized', 'hazardous')),
    delivery_cost DECIMAL NOT NULL CHECK(delivery_cost >= 0),
    order_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) NOT NULL CHECK(status IN ('created', 'in_progress', 'delivered', 'cancelled'))
);

CREATE TABLE IF NOT EXISTS logistics.Couriers (
    courier_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    gender VARCHAR(10) CHECK(gender IN ('male', 'female')),
    birth_date DATE NOT NULL CHECK(birth_date <= CURRENT_DATE - INTERVAL '18 years'),
    phone VARCHAR(20) UNIQUE NOT NULL,
    experience NUMERIC DEFAULT 0 CHECK(experience >= 0),
    orders_cnt INT DEFAULT 0 CHECK(orders_cnt >= 0),
    rating NUMERIC DEFAULT 0 CHECK(rating BETWEEN 0 AND 5),
    total_fine DECIMAL DEFAULT 0 CHECK(total_fine >= 0),
    total_bonus DECIMAL DEFAULT 0 CHECK(total_bonus >= 0),
    transport_id INT REFERENCES logistics.Transport(transport_id)
);

CREATE TABLE IF NOT EXISTS logistics.Delivery_logs (
    log_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES logistics.Orders(order_id),
    courier_id INT NOT NULL REFERENCES logistics.Couriers(courier_id),
    transport_id INT REFERENCES logistics.Transport(transport_id),
    stage_type VARCHAR(50) NOT NULL CHECK(stage_type IN ('transit', 'delivery')),
    src_wh_id INT REFERENCES logistics.Warehouse(warehouse_id),
    dst_wh_id INT REFERENCES logistics.Warehouse(warehouse_id),
    dest_addr VARCHAR(255),
    receive_dt TIMESTAMP CHECK(receive_dt < plan_dt AND receive_dt < actual_dt),
    plan_dt TIMESTAMP NOT NULL,
    actual_dt TIMESTAMP,
    stage_status VARCHAR(50) NOT NULL CHECK(stage_status IN ('pending', 'in_progress', 'completed', 'cancelled')),

    CHECK(
        (stage_type = 'transit' AND dst_wh_id IS NOT NULL) OR 
        (stage_type = 'delivery' AND dest_addr IS NOT NULL)
    )
);


CREATE TABLE IF NOT EXISTS logistics.Finances (
    operation_id SERIAL PRIMARY KEY,
    courier_id INT NOT NULL REFERENCES logistics.Couriers(courier_id),
    order_id INT NOT NULL REFERENCES logistics.Orders(order_id),
    log_id INT NOT NULL REFERENCES logistics.Delivery_logs(log_id),
    operation_type VARCHAR(50) NOT NULL CHECK(operation_type IN ('bonus', 'fine')),
    amount DECIMAL NOT NULL CHECK(amount > 0),
    created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
