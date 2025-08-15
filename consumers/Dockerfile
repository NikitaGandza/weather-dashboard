FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

copy main_consumer.py .

CMD ["python", "main_consumer.py"]