import json
import requests
import boto3
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import datetime

start_date = airflow.utils.dates.days_ago(2)  # Start date of the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}


def json_scraper(url, file_name, bucket):
    """Scrape the data from the given url and save it to the given file_name in an S3 bucket

    Args:
        url (str): The url to scrape the data from
        file_name (str): The name of the file to save the data to
        bucket (str): The name of the S3 bucket to save the data to
    """

    print("Scraping data from:", url)
    response = requests.request("GET", url)
    assert (
        response.status_code == 200
    ), f"Failed to scrape data\nStatus code: {response.status_code}   "
    json_data = response.json()
    print("Data scraped successfully")

    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=4)

    print("Data saved to:", file_name)


with DAG(
    "raw_predictit",
    default_args=default_args,
    description="A DAG to scrape the data from the PredictIt API",
    schedule_interval=datetime.timedelta(days=1),
    start_date=start_date,
    catchup=False,
    tags=["predictit"],
) as dag:

    extract_predictit = PythonOperator(
        task_id="extract_predictit",
        python_callable=json_scraper,
        op_kwargs={
            "url": "https://www.predictit.org/api/marketdata/all/",
            "file_name": "predicit_markets.json",
            "bucket": "data-mbfr",
        },
        dag=dag,
    )

ready = DummyOperator(task_id="ready")

extract_predictit >> ready
