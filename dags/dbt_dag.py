from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 25), # Change as needed
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'dbt_snowflake_workflow',
    default_args=default_args,
    description='Run dbt models using dbt Core',
    schedule_interval='@daily', # Run daily
    catchup=False,
)

# 1. Verifier que la connexion a Snowflake fonctionne
check_connection = BashOperator(
    task_id='check_snowflake_connection',
    bash_command='dbt debug --connection',
    dag=dag,
)

# 2. Construire les vues de staging (stg_customers, stg_orders, ...)
run_staging = BashOperator(
    task_id='dbt_run_staging',
    bash_command='dbt run --select staging',
    dag=dag,
)

# 3. Construire les tables marts (daily_order_revenue)
run_marts = BashOperator(
    task_id='dbt_run_marts',
    bash_command='dbt run --select marts',
    dag=dag,
)

# 4. Tester la qualite des donnees (tests/snowflake_test.yml)
test_models = BashOperator(
    task_id='dbt_test',
    bash_command='dbt test --select staging marts',
    dag=dag,
)

# Set the order of execution
check_connection >> run_staging >> run_marts >> test_models