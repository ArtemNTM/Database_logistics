# Версионирование для таблицы курьеров
CREATE TABLE IF NOT EXISTS logistics.couriers_history (
    history_id SERIAL PRIMARY KEY,
    courier_id INT NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    gender VARCHAR(10),
    birth_date DATE NOT NULL,
    phone VARCHAR(20) NOT NULL,
    experience NUMERIC,
    orders_cnt INT,
    rating NUMERIC,
    total_fine DECIMAL,
    total_bonus DECIMAL,
    transport_id INT,
    changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    operation_type VARCHAR(10) NOT NULL
);

CREATE OR REPLACE FUNCTION logistics.save_courier_history()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  BEGIN
    INSERT INTO logistics.couriers_history (
        courier_id, first_name, last_name, gender, birth_date, phone,
        experience, orders_cnt, rating, total_fine, total_bonus, transport_id,
        changed_at, operation_type
    )
    VALUES (
        OLD.courier_id, OLD.first_name, OLD.last_name, OLD.gender, OLD.birth_date, OLD.phone,
        OLD.experience, OLD.orders_cnt, OLD.rating, OLD.total_fine, OLD.total_bonus, OLD.transport_id,
        CURRENT_TIMESTAMP, TG_OP
    );

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    
    RETURN NEW;
  END;
$$;

CREATE TRIGGER trg_couriers_versioning
AFTER UPDATE OR DELETE
ON logistics.couriers
FOR EACH ROW
EXECUTE FUNCTION logistics.save_courier_history();

