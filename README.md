# Weather dashboard 

Data ingestion from Open-Meteo and visualizing it in Apache Superset

ToDo:
- Superset:
  - Create admin acc on docker compose up, install clickhouse driver as a separate file
    ```
    docker exec -it superset superset db upgrade
    docker exec -it superset superset init
    docker exec -it superset superset fab create-admin \
      --username admin \
      --firstname Admin \
      --lastname User \
      --email admin@example.com \
      --password admin 
    docker exec -it superset pip install clickhouse-connect 
    ```
