from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable

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
    try:
        # Variable.get() returns a string
        api_user_id = int(Variable.get("api_user_id", default_var="-1"))

        if api_user_id in (-1, 10):
            api_user_id = 1
        else:
            api_user_id += 1

        # Variable.set() requires a string
        Variable.set("api_user_id", str(api_user_id))

        logger.info("Current API User ID: %s", api_user_id)

        return api_user_id

    except Exception:
        logger.exception("Failed to set api_user_id")
        raise


# --------------------------------------------------
# Task 2 : Extract Posts
# --------------------------------------------------
def extract_userposts(ti, **kwargs):
    try:
        api_user_id = ti.xcom_pull(task_ids="set_api_user_id")

        logger.info("Fetching posts for userId=%s", api_user_id)

        response = requests.get(
            f"{API_BASE_URL}/posts",
            params={"userId": api_user_id},
            timeout=30,
        )

        response.raise_for_status()

        user_posts = response.json()

        logger.info("Fetched %s posts", len(user_posts))

        return user_posts

    except Exception:
        logger.exception("Failed to extract posts")
        raise


# --------------------------------------------------
# Task 3 : Write to Kinesis
# --------------------------------------------------
def write_userposts_to_stream(ti, **kwargs):
    try:
        stream_name = "user-posts-data-stream"

        user_posts = ti.xcom_pull(task_ids="extract_userposts")

        if not user_posts:
            logger.warning("No records returned from API.")
            return "No records to send."

        for post in user_posts:
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(post).encode("utf-8"),
                PartitionKey=str(post["userId"]),
            )

            logger.info(
                "Record %s written. Sequence=%s Shard=%s",
                post["id"],
                response["SequenceNumber"],
                response["ShardId"],
            )

        return f"{len(user_posts)} records written to {stream_name}"

    except Exception:
        logger.exception("Failed writing to Kinesis")
        raise


# --------------------------------------------------
# DAG
# --------------------------------------------------

default_args = {
    "owner": "Sovan",
}

with DAG(
    dag_id="load_api_aws_kinesis",
    default_args=default_args,
    start_date=datetime(2023, 9, 24),
    schedule="@daily",
    catchup=False,
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

    set_api_user_id_task >> extract_userposts_task >> write_userposts_task
