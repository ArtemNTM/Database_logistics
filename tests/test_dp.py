import os
import pytest
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

DB_DSN = os.getenv("DB_DSN", "postgresql://postgres:postgres@localhost:5432/second_db")

T_BASE    = datetime(2024, 6, 1, 12, 0, 0)
T_RECEIVE = T_BASE - timedelta(hours=2, minutes=10)
T_PLAN    = T_BASE
T_EARLY   = T_BASE - timedelta(hours=1)
T_LATE    = T_BASE + timedelta(hours=1)

F_BASE    = datetime(2030, 6, 1, 12, 0, 0)
F_RECEIVE = F_BASE - timedelta(hours=2, minutes=10)
F_PLAN    = F_BASE
F_ACTUAL  = F_BASE - timedelta(hours=1)


@pytest.fixture(scope="module")
def conn():
    c = psycopg2.connect(DB_DSN)
    c.autocommit = False
    yield c
    c.rollback()
    c.close()


@pytest.fixture
def cur(conn):
    c = conn.cursor(cursor_factory=RealDictCursor)
    c.execute("SAVEPOINT test_sp")
    try:
        yield c
    finally:
        c.execute("ROLLBACK TO SAVEPOINT test_sp")
        c.execute("RELEASE SAVEPOINT test_sp")
        c.close()


@pytest.fixture
def base_data(cur):
    cur.execute("""
        INSERT INTO logistics.clients (first_name, last_name, birth_date, city, phone, email)
        VALUES ('Тест', 'Отправитель', '1990-01-01', 'Москва', '+79991110001', 'tsender@test.io')
        RETURNING client_id
    """)
    sender_id = cur.fetchone()["client_id"]

    cur.execute("""
        INSERT INTO logistics.clients (first_name, last_name, birth_date, city, phone, email)
        VALUES ('Тест', 'Получатель', '1990-01-01', 'Казань', '+79991110002', 'treceiver@test.io')
        RETURNING client_id
    """)
    receiver_id = cur.fetchone()["client_id"]

    cur.execute("""
        INSERT INTO logistics.transport
            (transport_name, transport_type, payload_capacity, cost_per_km, units_cnt)
        VALUES ('Тест Фургон', 'van', 1000, 20.0, 1)
        RETURNING transport_id
    """)
    transport_id = cur.fetchone()["transport_id"]

    cur.execute("""
        INSERT INTO logistics.couriers
            (first_name, last_name, birth_date, phone, transport_id)
        VALUES ('Тест', 'Курьер', '1990-01-01', '+79991110003', %s)
        RETURNING courier_id
    """, (transport_id,))
    courier_id = cur.fetchone()["courier_id"]

    cur.execute("""
        INSERT INTO logistics.warehouse (city, street, building, capacity, current_load)
        VALUES ('Москва', 'Тестовая', '1', 1000.0, 100.0)
        RETURNING warehouse_id
    """)
    wh_src = cur.fetchone()["warehouse_id"]

    cur.execute("""
        INSERT INTO logistics.warehouse (city, street, building, capacity, current_load)
        VALUES ('Казань', 'Тестовая', '1', 1000.0, 100.0)
        RETURNING warehouse_id
    """)
    wh_dst = cur.fetchone()["warehouse_id"]

    cur.execute("""
        INSERT INTO logistics.orders
            (sender_id, receiver_id, volume, product_type, delivery_cost, order_date, status)
        VALUES (%s, %s, 10.0, 'standard', 500.0, %s, 'created')
        RETURNING order_id
    """, (sender_id, receiver_id, T_BASE - timedelta(hours=5)))
    order_id = cur.fetchone()["order_id"]

    return {
        "sender_id":    sender_id,
        "receiver_id":  receiver_id,
        "transport_id": transport_id,
        "courier_id":   courier_id,
        "wh_src":       wh_src,
        "wh_dst":       wh_dst,
        "order_id":     order_id,
    }


def _insert_delivery_log(cur, d, *, stage_type, stage_status,
                         plan_dt, actual_dt=None,
                         dest_addr=None, src_wh=None, dst_wh=None):
    cur.execute("""
        INSERT INTO logistics.delivery_logs
            (order_id, courier_id, transport_id, stage_type,
             dest_addr, src_wh_id, dst_wh_id,
             receive_dt, plan_dt, actual_dt, stage_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING log_id
    """, (d["order_id"], d["courier_id"], d["transport_id"], stage_type,
          dest_addr, src_wh, dst_wh,
          T_RECEIVE, plan_dt, actual_dt, stage_status))
    return cur.fetchone()["log_id"]


def test_trigger_bonus_on_early_delivery(cur, base_data):
    log_id = _insert_delivery_log(
        cur, base_data,
        stage_type="delivery", stage_status="completed",
        plan_dt=T_PLAN, actual_dt=T_EARLY,
        dest_addr="Тест ул. 1",
    )
    cur.execute(
        "SELECT operation_type, amount FROM logistics.finances WHERE log_id = %s",
        (log_id,),
    )
    row = cur.fetchone()
    assert row is not None
    assert row["operation_type"] == "bonus"
    assert float(row["amount"]) == pytest.approx((T_PLAN - T_EARLY).total_seconds() / 60 * 5.0)


def test_trigger_fine_on_late_delivery(cur, base_data):
    log_id = _insert_delivery_log(
        cur, base_data,
        stage_type="delivery", stage_status="completed",
        plan_dt=T_PLAN, actual_dt=T_LATE,
        dest_addr="Тест ул. 1",
    )
    cur.execute(
        "SELECT operation_type, amount FROM logistics.finances WHERE log_id = %s",
        (log_id,),
    )
    row = cur.fetchone()
    assert row is not None
    assert row["operation_type"] == "fine"
    assert float(row["amount"]) == pytest.approx((T_LATE - T_PLAN).total_seconds() / 60 * 10.0)


def test_trigger_no_finance_when_on_time(cur, base_data):
    log_id = _insert_delivery_log(
        cur, base_data,
        stage_type="delivery", stage_status="completed",
        plan_dt=T_PLAN, actual_dt=T_PLAN,
        dest_addr="Тест ул. 1",
    )
    cur.execute(
        "SELECT COUNT(*) AS cnt FROM logistics.finances WHERE log_id = %s",
        (log_id,),
    )
    assert cur.fetchone()["cnt"] == 0


def test_trigger_courier_total_bonus_updated(cur, base_data):
    d = base_data
    cur.execute(
        "SELECT total_bonus FROM logistics.couriers WHERE courier_id = %s",
        (d["courier_id"],),
    )
    before = float(cur.fetchone()["total_bonus"])

    _insert_delivery_log(
        cur, d,
        stage_type="delivery", stage_status="completed",
        plan_dt=T_PLAN, actual_dt=T_EARLY,
        dest_addr="Тест ул. 1",
    )

    cur.execute(
        "SELECT total_bonus FROM logistics.couriers WHERE courier_id = %s",
        (d["courier_id"],),
    )
    after = float(cur.fetchone()["total_bonus"])
    assert after == pytest.approx(before + (T_PLAN - T_EARLY).total_seconds() / 60 * 5.0)


def test_trigger_order_becomes_in_progress_on_log_insert(cur, base_data):
    d = base_data
    _insert_delivery_log(
        cur, d,
        stage_type="transit", stage_status="pending",
        plan_dt=T_PLAN,
        src_wh=d["wh_src"], dst_wh=d["wh_dst"],
    )
    cur.execute(
        "SELECT status FROM logistics.orders WHERE order_id = %s",
        (d["order_id"],),
    )
    assert cur.fetchone()["status"] == "in_progress"


def test_trigger_order_becomes_delivered(cur, base_data):
    d = base_data
    _insert_delivery_log(
        cur, d,
        stage_type="delivery", stage_status="completed",
        plan_dt=T_PLAN, actual_dt=T_EARLY,
        dest_addr="Тест ул. 1",
    )
    cur.execute(
        "SELECT status FROM logistics.orders WHERE order_id = %s",
        (d["order_id"],),
    )
    assert cur.fetchone()["status"] == "delivered"


def test_trigger_order_becomes_lost(cur, base_data):
    d = base_data
    _insert_delivery_log(
        cur, d,
        stage_type="delivery", stage_status="lost",
        plan_dt=T_PLAN,
        dest_addr="Тест ул. 1",
    )
    cur.execute(
        "SELECT status FROM logistics.orders WHERE order_id = %s",
        (d["order_id"],),
    )
    assert cur.fetchone()["status"] == "lost"


def test_trigger_warehouse_load_increases_on_completed_transit(cur, base_data):
    d = base_data
    cur.execute(
        "SELECT current_load FROM logistics.warehouse WHERE warehouse_id = %s",
        (d["wh_dst"],),
    )
    load_before = float(cur.fetchone()["current_load"])

    _insert_delivery_log(
        cur, d,
        stage_type="transit", stage_status="completed",
        plan_dt=T_PLAN, actual_dt=T_EARLY,
        src_wh=d["wh_src"], dst_wh=d["wh_dst"],
    )

    cur.execute(
        "SELECT current_load FROM logistics.warehouse WHERE warehouse_id = %s",
        (d["wh_dst"],),
    )
    load_after = float(cur.fetchone()["current_load"])
    assert load_after == pytest.approx(load_before + 10.0)


def test_trigger_courier_history_saved_on_update(cur, base_data):
    d = base_data
    cur.execute(
        "SELECT rating FROM logistics.couriers WHERE courier_id = %s",
        (d["courier_id"],),
    )
    old_rating = cur.fetchone()["rating"]

    cur.execute(
        "UPDATE logistics.couriers SET rating = 4.9 WHERE courier_id = %s",
        (d["courier_id"],),
    )

    cur.execute("""
        SELECT rating, operation_type
        FROM logistics.couriers_history
        WHERE courier_id = %s AND operation_type = 'UPDATE'
        ORDER BY history_id DESC LIMIT 1
    """, (d["courier_id"],))
    row = cur.fetchone()
    assert row is not None
    assert float(row["rating"]) == pytest.approx(float(old_rating))


def test_function_get_orders_statistics_by_period(cur, base_data):
    d = base_data
    cur.execute("""
        INSERT INTO logistics.orders
            (sender_id, receiver_id, volume, product_type, delivery_cost, order_date, status)
        VALUES (%s, %s, 5.0, 'fragile', 300.0, %s, 'created')
        RETURNING order_id
    """, (d["sender_id"], d["receiver_id"], F_BASE - timedelta(hours=5)))
    forder_id = cur.fetchone()["order_id"]

    cur.execute("""
        INSERT INTO logistics.delivery_logs
            (order_id, courier_id, transport_id, stage_type, dest_addr,
             receive_dt, plan_dt, actual_dt, stage_status)
        VALUES (%s, %s, %s, 'delivery', 'Тест ул. 1', %s, %s, %s, 'completed')
    """, (forder_id, d["courier_id"], d["transport_id"], F_RECEIVE, F_PLAN, F_ACTUAL))

    left  = (F_BASE - timedelta(days=1)).date()
    right = (F_BASE + timedelta(days=1)).date()
    cur.execute("""
        SELECT * FROM logistics.get_orders_statistics_by_period(%s, %s)
        WHERE product_type = 'fragile'
    """, (left, right))
    rows = cur.fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert row["orders_count"]  == 1
    assert row["delivered_num"] == 1
    assert row["cancelled_num"] == 0
    assert float(row["success_percent"]) == pytest.approx(100.0)


def test_function_get_courier_statistics_by_period(cur, base_data):
    d = base_data
    _insert_delivery_log(
        cur, d,
        stage_type="delivery", stage_status="completed",
        plan_dt=T_PLAN, actual_dt=T_EARLY,
        dest_addr="Тест ул. 1",
    )
    cur.execute("""
        SELECT * FROM logistics.get_courier_statistics_by_period(NULL, NULL)
        WHERE courier_id = %s
    """, (d["courier_id"],))
    row = cur.fetchone()
    assert row is not None
    assert row["total_deliveries"]   == 1
    assert row["on_time_deliveries"] == 1
    assert row["late_deliveries"]    == 0
    assert row["lost_deliveries"]    == 0


def test_view_lost_orders_investigation(cur, base_data):
    d = base_data
    _insert_delivery_log(
        cur, d,
        stage_type="delivery", stage_status="lost",
        plan_dt=T_PLAN,
        dest_addr="Тест ул. 1",
    )
    cur.execute("""
            SELECT order_id, last_known_stage, last_courier_id
            FROM logistics.v_lost_orders_investigation
            WHERE order_id = %s
    """, (d["order_id"],))
    row = cur.fetchone()
    assert row is not None
    assert row["order_id"]         == d["order_id"]
    assert row["last_known_stage"] == "delivery"
    assert row["last_courier_id"]  == d["courier_id"]
