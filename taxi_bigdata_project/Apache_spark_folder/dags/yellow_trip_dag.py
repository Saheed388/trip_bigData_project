from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys
import logging
from dotenv import load_dotenv
from datetime import datetime
from include.dbt_trigger import trigger_dbt_job

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Add include directory to Python path
include_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'include'))
sys.path.append(include_path)
logger.info(f"Added include path to sys.path: {include_path}")

# Load environment variables
load_dotenv()

# Define Python callables
def run_extract_trip(year, module_name, **context):
    try:
        module = __import__(module_name)
        logger.info(f"Starting data extraction for {year}")
        result = module.download_files(year=year)
        logger.info(f"Data extraction for {year} completed successfully")
        return result
    except ImportError as e:
        logger.error(f"Import error in run_extract_trip for {year}: {str(e)}")
        logger.error(f"Current working directory: {os.getcwd()}")
        logger.error(f"Files in include directory: {os.listdir(include_path) if os.path.exists(include_path) else 'Include directory not found'}")
        raise
    except Exception as e:
        logger.error(f"Error in run_extract_trip for {year}: {str(e)}")
        raise

def run_process_to_parquet(year, module_name, **context):
    try:
        module = __import__(module_name)
        logger.info(f"Starting data processing for {year}")
        result = module.process_to_parquet(year=year)
        logger.info(f"Data processing for {year} completed successfully")
        return result
    except ImportError as e:
        logger.error(f"Import error in run_process_to_parquet for {year}: {str(e)}")
        logger.error(f"Current working directory: {os.getcwd()}")
        logger.error(f"Files in include directory: {os.listdir(include_path) if os.path.exists(include_path) else 'Include directory not found'}")
        raise
    except Exception as e:
        logger.error(f"Error in run_process_to_parquet for {year}: {str(e)}")
        raise

def run_transform_taxi_data(year, module_name, **context):
    try:
        module = __import__(module_name)
        parquet_path = context['task_instance'].xcom_pull(task_ids=f'process_to_parquet_{year}')
        logger.info(f"Starting data transformation for {year}")
        result = module.transform_taxi_data(parquet_path, year=year)
        logger.info(f"Data transformation for {year} completed successfully")
        return result
    except ImportError as e:
        logger.error(f"Import error in run_transform_taxi_data for {year}: {str(e)}")
        logger.error(f"Current working directory: {os.getcwd()}")
        logger.error(f"Files in include directory: {os.listdir(include_path) if os.path.exists(include_path) else 'Include directory not found'}")
        raise
    except Exception as e:
        logger.error(f"Error in run_transform_taxi_data for {year}: {str(e)}")
        raise

def run_load_to_snowflake(year, module_name, **context):
    try:
        module = __import__(module_name)
        parquet_path = context['task_instance'].xcom_pull(task_ids=f'transform_taxi_data_{year}')
        logger.info(f"Starting data load to Snowflake for {year}")
        module.load_to_snowflake(parquet_path, snowflake_conn_id='snowflakes_con')
        logger.info(f"Data load to Snowflake for {year} completed successfully")
    except ImportError as e:
        logger.error(f"Import error in run_load_to_snowflake for {year}: {str(e)}")
        logger.error(f"Current working directory: {os.getcwd()}")
        logger.error(f"Files in include directory: {os.listdir(include_path) if os.path.exists(include_path) else 'Include directory not found'}")
        raise
    except Exception as e:
        logger.error(f"Error in run_load_to_snowflake for {year}: {str(e)}")
        raise

def run_notify_dag_start(**context):
    try:
        from notifications import notify_dag_start
        logger.info("Sending DAG start notification")
        notify_dag_start()
        logger.info("DAG start notification sent successfully")
    except ImportError as e:
        logger.warning(f"Import error in run_notify_dag_start: {str(e)}. Skipping notification.")
    except Exception as e:
        logger.error(f"Error in run_notify_dag_start: {str(e)}")
        raise

def run_notify_dag_complete(**context):
    try:
        from notifications import notify_dag_complete
        logger.info("Sending DAG completion notification")
        notify_dag_complete(context)
        logger.info("DAG completion notification sent successfully")
    except ImportError as e:
        logger.warning(f"Import error in run_notify_dag_complete: {str(e)}. Skipping notification.")
    except Exception as e:
        logger.error(f"Error in run_notify_dag_complete: {str(e)}")
        raise

def notify_failure(context):
    try:
        from notifications import notify_task_failure
        logger.info("Sending task failure notification")
        notify_task_failure(context)
    except ImportError as e:
        logger.warning(f"Import error in notify_failure: {str(e)}. Skipping notification.")
    except Exception as e:
        logger.error(f"Error in notify_failure: {str(e)}")
        raise

def notify_success(context):
    try:
        from notifications import notify_task_success
        logger.info("Sending task success notification")
        notify_task_success(context)
    except ImportError as e:
        logger.warning(f"Import error in notify_success: {str(e)}. Skipping notification.")
    except Exception as e:
        logger.error(f"Error in notify_success: {str(e)}")
        raise

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_failure,
    'on_success_callback': notify_success,
}

# Define the DAG
with DAG(
    dag_id='taxi_bigdata_etl_dag',
    default_args=default_args,
    description='ETL for NYC Yellow Taxi data (2019-2021)',
    schedule='0 12 * * *',
    start_date=datetime(2025, 8, 3),
    catchup=False,
    tags=['taxi', 'data'],
    max_active_runs=1,
) as dag:
    # Define tasks
    start_notification = PythonOperator(
        task_id='notify_dag_start',
        python_callable=run_notify_dag_start
    )

    # Tasks for 2019
    extract_2019 = PythonOperator(
        task_id='extract_trip_2019',
        python_callable=run_extract_trip,
        op_kwargs={'year': 2019, 'module_name': 'yellow_2019'}
    )

    process_data_2019 = PythonOperator(
        task_id='process_to_parquet_2019',
        python_callable=run_process_to_parquet,
        op_kwargs={'year': 2019, 'module_name': 'yellow_2019'}
    )

    transform_data_2019 = PythonOperator(
        task_id='transform_taxi_data_2019',
        python_callable=run_transform_taxi_data,
        op_kwargs={'year': 2019, 'module_name': 'yellow_2019'}
    )

    load_to_snowflake_2019 = PythonOperator(
        task_id='load_transformed_data_to_snowflake_2019',
        python_callable=run_load_to_snowflake,
        op_kwargs={'year': 2019, 'module_name': 'yellow_2019'}
    )

    # Tasks for 2020
    extract_2020 = PythonOperator(
        task_id='extract_trip_2020',
        python_callable=run_extract_trip,
        op_kwargs={'year': 2020, 'module_name': 'yellow_2020'}
    )

    process_data_2020 = PythonOperator(
        task_id='process_to_parquet_2020',
        python_callable=run_process_to_parquet,
        op_kwargs={'year': 2020, 'module_name': 'yellow_2020'}
    )

    transform_data_2020 = PythonOperator(
        task_id='transform_taxi_data_2020',
        python_callable=run_transform_taxi_data,
        op_kwargs={'year': 2020, 'module_name': 'yellow_2020'}
    )

    load_to_snowflake_2020 = PythonOperator(
        task_id='load_transformed_data_to_snowflake_2020',
        python_callable=run_load_to_snowflake,
        op_kwargs={'year': 2020, 'module_name': 'yellow_2020'}
    )

    # Tasks for 2021
    extract_2021 = PythonOperator(
        task_id='extract_trip_2021',
        python_callable=run_extract_trip,
        op_kwargs={'year': 2021, 'module_name': 'yellow_2021'}
    )

    process_data_2021 = PythonOperator(
        task_id='process_to_parquet_2021',
        python_callable=run_process_to_parquet,
        op_kwargs={'year': 2021, 'module_name': 'yellow_2021'}
    )

    transform_data_2021 = PythonOperator(
        task_id='transform_taxi_data_2021',
        python_callable=run_transform_taxi_data,
        op_kwargs={'year': 2021, 'module_name': 'yellow_2021'}
    )

    load_to_snowflake_2021 = PythonOperator(
        task_id='load_transformed_data_to_snowflake_2021',
        python_callable=run_load_to_snowflake,
        op_kwargs={'year': 2021, 'module_name': 'yellow_2021'}
    )

    # dbt Cloud trigger
    trigger_dbt_job_task = PythonOperator(
        task_id="trigger_dbt_job",
        python_callable=trigger_dbt_job,
        on_failure_callback=notify_failure,
        on_success_callback=notify_success,
    )

    complete_notification = PythonOperator(
        task_id='notify_dag_complete',
        python_callable=run_notify_dag_complete
    )

    # Set task dependencies for concurrent execution
    start_notification >> [extract_2019, extract_2020, extract_2021]

    # 2019 pipeline
    extract_2019 >> process_data_2019 >> transform_data_2019 >> load_to_snowflake_2019

    # 2020 pipeline
    extract_2020 >> process_data_2020 >> transform_data_2020 >> load_to_snowflake_2020

    # 2021 pipeline
    extract_2021 >> process_data_2021 >> transform_data_2021 >> load_to_snowflake_2021

    [load_to_snowflake_2019, load_to_snowflake_2020, load_to_snowflake_2021] >> trigger_dbt_job_task >> complete_notification