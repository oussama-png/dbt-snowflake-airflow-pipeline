FROM apache/airflow:2.10.2

# dbt + Snowflake dans un venv separe (evite les conflits avec les dependances d'Airflow)
USER root
RUN python -m venv /opt/dbt_venv \
 && /opt/dbt_venv/bin/pip install --no-cache-dir dbt-core==1.12.5 dbt-snowflake==1.12.1 \
 && ln -s /opt/dbt_venv/bin/dbt /usr/local/bin/dbt
USER airflow
