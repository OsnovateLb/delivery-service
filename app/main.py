# app/simulator.py
import os
import time
import random
from datetime import datetime, timezone, timedelta
import psycopg2
from faker import Faker

fake = Faker('ru_RU')

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'delivery'),
    'user': os.getenv('DB_USER', 'user'),
    'password': os.getenv('DB_PASSWORD', 'password')
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def init_sample_data():
    conn = get_connection()
    cur = conn.cursor()

    # Клиенты
    cur.execute("SELECT COUNT(*) FROM customers")
    if cur.fetchone()[0] == 0:
        for _ in range(10):
            cur.execute(
                "INSERT INTO customers (name, phone) VALUES (%s, %s)",
                (fake.name(), fake.phone_number())
            )

    # Рестораны
    cur.execute("SELECT COUNT(*) FROM restaurants")
    if cur.fetchone()[0] == 0:
        restos = [
            ("Пицца Хат", "ул. Ленина, 10"),
            ("Суши Вок", "пр. Мира, 25"),
            ("Бургер Кинг", "ул. Гагарина, 5"),
            ("Кофе Бар", "наб. Реки, 12"),
            ("Вок & Wok", "ул. Советская, 33")
        ]
        cur.executemany(
            "INSERT INTO restaurants (name, address) VALUES (%s, %s)",
            restos
        )

    # Курьеры
    cur.execute("SELECT COUNT(*) FROM couriers")
    if cur.fetchone()[0] == 0:
        for _ in range(5):
            cur.execute(
                "INSERT INTO couriers (name, phone, is_available) VALUES (%s, %s, %s)",
                (fake.name(), fake.phone_number(), True)
            )

    conn.commit()
    cur.close()
    conn.close()

def create_new_order():
    """Создаёт новый заказ со статусом 'created'."""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT id FROM customers")
    customers = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT id FROM restaurants")
    restaurants = [r[0] for r in cur.fetchall()]

    if not customers or not restaurants:
        cur.close()
        conn.close()
        return

    customer_id = random.choice(customers)
    restaurant_id = random.choice(restaurants)
    order_time = datetime.now(timezone.utc)

    cur.execute("""
        INSERT INTO orders (customer_id, restaurant_id, order_time, status)
        VALUES (%s, %s, %s, 'created')
        RETURNING id
    """, (customer_id, restaurant_id, order_time))

    order_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()


def assign_couriers_to_ready_orders():
    """Назначает курьеров на заказы, которые ждут > 10 сек и ещё не в доставке."""
    conn = get_connection()
    cur = conn.cursor()

    threshold = datetime.now(timezone.utc) - timedelta(seconds=10)

    cur.execute("""
        SELECT id FROM orders
        WHERE status = 'created' AND order_time <= %s
        ORDER BY order_time ASC
    """, (threshold,))

    old_orders = [r[0] for r in cur.fetchall()]

    if not old_orders:
        cur.close()
        conn.close()
        return

    cur.execute("SELECT id FROM couriers WHERE is_available = true")
    available_couriers = [r[0] for r in cur.fetchall()]

    if not available_couriers:
        cur.close()
        conn.close()
        return

    assigned = 0
    for order_id in old_orders:
        if not available_couriers:
            break
        courier_id = available_couriers.pop()
        assigned_at = datetime.now(timezone.utc)

        cur.execute("UPDATE couriers SET is_available = false WHERE id = %s", (courier_id,))
        cur.execute("""
            INSERT INTO deliveries (order_id, courier_id, assigned_at)
            VALUES (%s, %s, %s)
        """, (order_id, courier_id, assigned_at))
        cur.execute("UPDATE orders SET status = 'in_delivery' WHERE id = %s", (order_id,))

        assigned += 1

    conn.commit()
    cur.close()
    conn.close()

def complete_deliveries():
    """Завершает доставки, которые в пути > 15 сек."""
    conn = get_connection()
    cur = conn.cursor()

    threshold = datetime.now(timezone.utc) - timedelta(seconds=15)

    cur.execute("""
        SELECT d.order_id, d.courier_id
        FROM deliveries d
        JOIN orders o ON d.order_id = o.id
        WHERE o.status = 'in_delivery' AND d.assigned_at <= %s
    """, (threshold,))

    ready_deliveries = cur.fetchall()

    for order_id, courier_id in ready_deliveries:
        delivered_at = datetime.now(timezone.utc)

        cur.execute("""
            UPDATE deliveries
            SET delivered_at = %s
            WHERE order_id = %s
        """, (delivered_at, order_id))

        cur.execute("UPDATE orders SET status = 'delivered' WHERE id = %s", (order_id,))

        cur.execute("UPDATE couriers SET is_available = true WHERE id = %s", (courier_id,))


    if ready_deliveries:
        conn.commit()

    cur.close()
    conn.close()

def main_loop():
    while True:
        try:
            if random.random() < 0.6:
                create_new_order()

            assign_couriers_to_ready_orders()

            complete_deliveries()

            time.sleep(3)
        except KeyboardInterrupt:
            print("Симуляция остановлена.")
            break
        except Exception as e:
            print(f"Ошибка: {e}")
            time.sleep(3)

if __name__ == "__main__":
    time.sleep(5)
    init_sample_data()
    main_loop()