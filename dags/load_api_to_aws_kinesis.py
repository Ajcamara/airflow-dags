from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime
import boto3
import requests
import json
import logging

logger = logging.getLogger(__name__)

API_BASE_URL = "https://jsonplaceholder.typicode.com"

kinesis_client = boto3.client("kinesis")


# --------------------------------------------------
# Task 1 : Increment User ID
# --------------------------------------------------
def set_api_user_id(**kwargs):

    api_user_id = int(Variable.get("api_user_id", default_var=-1))

    if api_user_id in (-1, 10):
        api_user_id = 1
    else:
        api_user_id += 1

    Variable.set("api_user_id", api_user_id)

    logger.info("Current API User ID: %s", api_user_id)

    return api_user_id


# --------------------------------------------------
# Task 2 : Extract Posts
# --------------------------------------------------
def extract_userposts(ti, **kwargs):

    api_user_id = ti.xcom_pull(
        task_ids="set_api_user_id"
    )

    response = requests.get(
        f"{API_BASE_URL}/posts",
        params={"userId": api_user_id},
        timeout=30,
    )

    response.raise_for_status()

    user_posts = response.json()

    logger.info("Fetched %s posts", len(user_posts))

    return user_posts


# --------------------------------------------------
# Task 3 : Write to Kinesis
# --------------------------------------------------
def write_userposts_to_stream(ti, **kwargs):

    stream_name = "user-posts-data-stream"

    user_posts = ti.xcom_pull(
        task_ids="extract_userposts"
    )

    for post in user_posts:

        response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(post).encode("utf-8"),
            PartitionKey=str(post["userId"]),
        )

        logger.info(
            "SequenceNumber=%s Shard=%s",
            response["SequenceNumber"],
            response["ShardId"],
        )

    return f"{len(user_posts)} records loaded to {stream_name}"


# --------------------------------------------------
# DAG
# --------------------------------------------------

default_args = {
    "owner": "Sovan",
}

with DAG(
    dag_id="load_api_aws_kinesis",
    start_date=datetime(2023, 9, 24),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["kinesis", "api"],
) as dag:

    set_api_user_id_task = PythonOperator(
        task_id="set_api_user_id",
        python_callable=set_api_user_id,
    )

    extract_userposts_task = PythonOperator(
        task_id="extract_userposts",
        python_callable=extract_userposts,
    )

    write_userposts_task = PythonOperator(
        task_id="write_userposts_to_stream",
        python_callable=write_userposts_to_stream,
    )

    (
        set_api_user_id_task
        >> extract_userposts_task
        >> write_userposts_task
    )
