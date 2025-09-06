from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv
import os
import requests
import json

# Load env vars
load_dotenv()

DBT_ACCOUNT_ID = os.getenv("DBT_ACCOUNT_ID", "704719")   # default to your value
DBT_JOB_ID = os.getenv("DBT_JOB_ID", "7047106")          # default to your value
DBT_API_KEY = os.getenv("DBT_API_KEY")                   # must be set in .env

def trigger_dbt_job():
    if not DBT_API_KEY:
        raise ValueError(" DBT_API_KEY must be set in your .env or Airflow Variables")

    url = f"https://bd147.us1.dbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/jobs/{DBT_JOB_ID}/run/"

    headers = {
        "Authorization": f"Token {DBT_API_KEY}",
        "Content-Type": "application/json",
    }

    body = {
        "cause": "Triggered via Airflow API"
    }

    response = requests.post(url, headers=headers, data=json.dumps(body))

    if response.status_code != 200:
        raise Exception(f" Failed to trigger dbt job: {response.text}")

    run_info = response.json()
    run_id = run_info.get("data", {}).get("id")

    print(f" Triggered dbt job {DBT_JOB_ID} (Run ID: {run_id})")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="trigger_dbt_cloud_job",
    default_args=default_args,
    schedule=None,  # run manually
    catchup=False,
) as dag:

    trigger_job = PythonOperator(
        task_id="trigger_dbt_job",
        python_callable=trigger_dbt_job,
    )

    trigger_job
