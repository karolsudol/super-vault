   # Dockerfile
   FROM fishtownanalytics/dbt:1.0.0

   # Install dbt-clickhouse
   RUN pip install dbt-clickhouse