from dotenv import load_dotenv
import os
import requests
import json
import time

# Load env vars
load_dotenv()

DBT_ACCOUNT_ID = os.getenv("DBT_ACCOUNT_ID")   # default to your value
DBT_JOB_ID = os.getenv("DBT_JOB_ID")          # default to your value
DBT_API_KEY = os.getenv("DBT_API_KEY")                   # must be set in .env

def trigger_dbt_job():
    if not DBT_API_KEY:
        raise ValueError("DBT_API_KEY must be set in your .env or Airflow Variables")

    url = f"https://bd147.us1.dbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/jobs/{DBT_JOB_ID}/run/"
    headers = {
        "Authorization": f"Token {DBT_API_KEY}",
        "Content-Type": "application/json",
    }
    body = {
        "cause": "Triggered via Airflow API"
    }

    # Trigger the dbt job
    response = requests.post(url, headers=headers, data=json.dumps(body))
    if response.status_code != 200:
        raise Exception(f"Failed to trigger dbt job: {response.text}")

    run_info = response.json()
    run_id = run_info.get("data", {}).get("id")
    print(f"Triggered dbt job {DBT_JOB_ID} (Run ID: {run_id})")

    # Poll for job completion
    status_url = f"https://bd147.us1.dbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/runs/{run_id}/"
    while True:
        status_response = requests.get(status_url, headers=headers)
        if status_response.status_code != 200:
            raise Exception(f"Failed to fetch status: {status_response.text}")

        status_data = status_response.json()
        status = status_data.get("data", {}).get("status")

        # dbt Cloud numeric status codes
        status_map = {
            1: "Queued",
            2: "Starting",
            3: "Running",
            10: "Success",
            20: "Error",
            30: "Cancelled",
        }
        print(f"Run {run_id} status: {status_map.get(status, status)}")

        if status == 10:
            print(f"dbt job {DBT_JOB_ID} (Run ID: {run_id}) completed successfully ✅")
            return run_id
        elif status in [20, 30]:
            raise Exception(f"dbt job failed with status: {status_map.get(status, status)}")

        time.sleep(30)  # wait before polling again
