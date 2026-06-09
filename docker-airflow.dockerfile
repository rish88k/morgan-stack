FROM apache/airflow:2.9.3
USER airflow
RUN pip install --upgrade pip
RUN pip install --no-cache-dir \
  apache-airflow==2.9.3 \
  apache-airflow-providers-snowflake \
  apache-airflow-providers-standard \
  dbt-snowflake \
  numpy pandas
