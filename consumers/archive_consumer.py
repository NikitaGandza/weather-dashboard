import psycopg2
import time
from kafka import KafkaConsumer
import os
import json
from datetime import datetime


for attempt in range(10):
    try:
        client_archive = psycopg2.connect(
            host="postgres-archive",
            port=5432,
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            dbname=os.getenv("POSTGRES_DB")
        )
        time.sleep(2)
        break
    except Exception as e:
        print(f"Postgres not ready yet ({e}), retrying...")
        time.sleep(2)



consumer = KafkaConsumer(
    'dummy-data',
    bootstrap_servers="redpanda:9092",
    group_id="dummy-reader-2",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)


create_table_query = """
CREATE TABLE IF NOT EXISTS public.leads (
    id INTEGER,
    timestamp TIMESTAMP,
    user_id TEXT,
    event_type TEXT,
    product_id TEXT,
    price REAL,
    quantity SMALLINT,
    location TEXT,
    device TEXT,
    session_id TEXT
);
"""
cur = client_archive.cursor()
cur.execute(create_table_query)
client_archive.commit()

for message in consumer:
    data = message.value

    insert_query = """
    INSERT INTO public.leads 
    (id, timestamp, user_id, event_type, product_id, price, quantity, location, device, session_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    cur = client_archive.cursor()
    cur.execute (
        insert_query,
        (
            data["id"],
            datetime.fromisoformat(data["timestamp"]),
            data['user_id'],
            data['event_type'],
            data['product_id'],
            data['price'],
            data['quantity'],
            data['location'],
            data['device'],
            data['session_id']
    ),
    )
    client_archive.commit()
