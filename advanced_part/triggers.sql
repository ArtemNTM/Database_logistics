# Триггер для обновления 
CREATE OR REPLACE FUNCTION logistics.insert_finance_operation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  DECLARE
    operation VARCHAR(50);
    amount DECIMAL;
  BEGIN
    IF TG_OP = 'UPDATE' THEN
      IF OLD.stage_status = 'completed' THEN
        RETURN NEW;
      END IF;
    END IF;

    IF NEW.actual_dt IS NULL OR NEW.plan_dt = NEW.actual_dt THEN
      RETURN NEW;
    ELSEIF NEW.plan_dt > NEW.actual_dt THEN
      operation := 'bonus';
      amount := (EXTRACT(EPOCH FROM AGE(NEW.plan_dt, NEW.actual_dt)) / 60) * 5.0;
    ELSE
      operation := 'fine';
      amount := (EXTRACT(EPOCH FROM AGE(NEW.actual_dt, NEW.plan_dt)) / 60) * 10.0;
    END IF;
    INSERT INTO logistics.finances (courier_id, order_id, log_id, operation_type, amount, created_time)
    VALUES (NEW.courier_id, NEW.order_id, NEW.log_id, operation, amount, CURRENT_TIMESTAMP);

    RETURN NEW;
  END;
$$;

CREATE TRIGGER trg_new_fin_operation
AFTER INSERT OR UPDATE
ON logistics.delivery_logs
FOR EACH ROW
WHEN (NEW.stage_status = 'completed')
EXECUTE FUNCTION logistics.insert_finance_operation();


# Обновление total_bonus, total_fine у курьеров
CREATE OR REPLACE FUNCTION logistics.update_total_bonus_fine()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  BEGIN
    CASE NEW.operation_type
    WHEN 'bonus' THEN
      UPDATE logistics.couriers
      SET total_bonus = total_bonus + NEW.amount
      WHERE courier_id = NEW.courier_id;
    WHEN 'fine' THEN
      UPDATE logistics.couriers
      SET total_fine = total_fine + NEW.amount
      WHERE courier_id = NEW.courier_id;
    END CASE;
    RETURN NEW;
  END;
$$;

CREATE TRIGGER trg_courier_bonus_fine
AFTER INSERT
ON logistics.finances
FOR EACH ROW
EXECUTE FUNCTION logistics.update_total_bonus_fine();


# 3 Обновление вместимости складов

CREATE OR REPLACE FUNCTION logistics.warehouse_capacity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  BEGIN
    IF TG_OP = 'UPDATE' THEN
      IF OLD.stage_status = NEW.stage_status THEN
        RETURN NEW;
      END IF;
    END IF;

    IF NEW.stage_status = 'pending' THEN
      UPDATE logistics.warehouse
      SET current_load = current_load - (SELECT volume FROM logistics.orders WHERE order_id = NEW.order_id)
      WHERE NEW.src_wh_id = warehouse_id;
      RETURN NEW;
    END IF;

    IF NEW.stage_status = 'completed' AND NEW.stage_type = 'transit' THEN
      UPDATE logistics.warehouse
      SET current_load = current_load + (SELECT volume FROM logistics.orders WHERE order_id = NEW.order_id)
      WHERE NEW.dst_wh_id = warehouse_id;
      RETURN NEW;
    END IF;

    RETURN NEW;
  END;
$$;

CREATE TRIGGER trg_update_warehouse_capacity
AFTER INSERT OR UPDATE
ON logistics.delivery_logs
FOR EACH ROW
EXECUTE FUNCTION logistics.warehouse_capacity();

# 4 Обновление статуса заказа в orders

CREATE OR REPLACE FUNCTION logistics.update_order_status()
RETURNS trigger
LANGUAGE plpgsql
AS $$
  BEGIN
    IF TG_OP = 'INSERT' THEN
      UPDATE logistics.orders
      SET status = 'in_progress'
      WHERE order_id = NEW.order_id AND status = 'created';
    END IF;

    IF NEW.stage_status = 'lost' THEN
      UPDATE logistics.orders
      SET status = 'lost'
      WHERE order_id = NEW.order_id;
      RETURN NEW;
    END IF;

    IF NEW.stage_type = 'delivery' AND NEW.stage_status = 'completed' THEN
      UPDATE logistics.orders
      SET status = 'delivered'
      WHERE order_id = NEW.order_id;
      RETURN NEW;
    END IF;

    RETURN NEW;
  END;
$$;

CREATE TRIGGER trg_update_order_status
AFTER INSERT OR UPDATE
ON logistics.delivery_logs
FOR EACH ROW
EXECUTE FUNCTION logistics.update_order_status();