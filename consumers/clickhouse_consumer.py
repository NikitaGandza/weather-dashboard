from kafka import KafkaConsumer
import clickhouse_connect
import os
from datetime import datetime
import time
import json

for attempt in range(10):
    try:
        client = clickhouse_connect.get_client(
            host='clickhouse',
            port='8123',
            username=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            database=os.getenv("CLICKHOUSE_DB")
        )
        break
    except Exception as e:
        print(f"ClickHouse not ready yet ({e}), retrying...")
        time.sleep(2)

consumer = KafkaConsumer(
    'dummy-data',
    bootstrap_servers="redpanda:9092",
    group_id="dummy-reader-1",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

client.command(
    """		CREATE TABLE IF NOT EXISTS default.leads (
        id UInt32,
        timestamp DateTime,
        user_id String,
        event_type String,
        product_id String,
        price Float32,
        quantity UInt8,
        location String,
        device String,
        session_id String
    ) ENGINE = MergeTree()
    ORDER BY timestamp
    TTL timestamp + INTERVAL 1 MONTH DELETE;
"""
)

for message in consumer:
    data = message.value
    print(f"Data here {data}")

    client.insert(
        "default.leads",
        [[
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
        ]],
        column_names=[
            "id",
            "timestamp",
            "user_id",
            "event_type",
            "product_id",
            "price" ,
            "quantity" ,
            "location",
            "device",
            "session_id"
            ]
    )