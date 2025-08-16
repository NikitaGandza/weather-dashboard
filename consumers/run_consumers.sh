#!/bin/bash

# Run both scripts in background
# python clickhouse_consumer.py &
python archive_consumer.py &

# Wait for both to finish (they will run indefinitely)
wait
