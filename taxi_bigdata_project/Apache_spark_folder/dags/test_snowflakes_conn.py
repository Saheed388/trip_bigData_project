from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# Replace with the Snowflake connection ID you set up in Airflow UI
SNOWFLAKE_CONN_ID = "snowflake_default"

with DAG(
    dag_id="test_snowflake_connection",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # run manually
    catchup=False,
    tags=["test", "snowflake"],
) as dag:

    test_connection = SnowflakeOperator(
        task_id="run_test_query",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="SELECT CURRENT_TIMESTAMP;",
    )

    test_connection
