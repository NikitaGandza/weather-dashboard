FROM python:3.11-slim

WORKDIR /app

copy requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dbt_project.yml .
COPY profiles.yml /root/.dbt/profiles.yml
COPY models ./models


CMD ["dbt"]