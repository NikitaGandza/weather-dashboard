FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

copy clickhouse_consumer.py .
COPY archive_consumer.py .
COPY run_consumers.sh .

CMD ["sh", "run_consumers.sh"]