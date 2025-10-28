import os
import boto3
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

from airflow import DAG
from airflow.decorators import task
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log
# defining the default arguments for DAG
default_args = {
    "owner": "wkt7ne",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# defining the DAG task
# schedule is set to None for manual triggering
# start date is set to the past to allow immediate triggering
with DAG(
    dag_id="quote_assembler_dag",
    description="ds3022 project 2 airflow dag",
    start_date=datetime(2025, 10, 27),  
    schedule=None,                      
    catchup=False,
    default_args=default_args,
):

    # task one: fetching SQS url 
    @task
    def fetch_sqs_url():
        logger.info("Fetching SQS URL...")
        url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/wkt7ne"
        payload = requests.post(url).json()
        sqs_url = payload["sqs_url"]
        logger.info(f"Fetched SQS URL: {sqs_url}")
        return sqs_url

    # task two: getting SQS attributes
    @task
    def get_sqs_attributes(sqs_url: str):
        logger.info("Loading AWS credentials...")
        load_dotenv()
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        REGION_NAME = os.getenv("REGION_NAME", "us-east-1")

        sqs = boto3.client(
            "sqs",
            region_name=REGION_NAME,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        attributes = [
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ]
        response = sqs.get_queue_attributes(QueueUrl=sqs_url, AttributeNames=attributes)
        attribute_content = response["Attributes"]
        print("SQS Queue Attributes:", attribute_content)
        logger.info(f"Fetched SQS Queue Attributes: {attribute_content}")
        # Do not return boto3 client; nothing to return here

    # task three: building pairs from SQS messages & continuous monitoring of SQS message attributes
    @task
    def build_pairs(sqs_url: str, expected_count: int = 21):
        logger.info("Building pairs...")
        load_dotenv()
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        REGION_NAME = os.getenv("REGION_NAME", "us-east-1")

        sqs = boto3.client(
            "sqs",
            region_name=REGION_NAME,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

        collected_messages = []
        while len(collected_messages) < expected_count:
            try:
                response = sqs.receive_message(
                    QueueUrl=sqs_url,
                    MaxNumberOfMessages=10,
                    MessageAttributeNames=["All"],
                    WaitTimeSeconds=5,
                )
                logger.info("Received messages from SQS queue.")
            except Exception as e:
                logger.error(f"Error receiving messages: {e}")
                print("Error receiving messages:", e)
                continue

            for message in response.get("Messages", []):
                attributes = message.get("MessageAttributes", {})
                order_num = attributes.get("order_no", {}).get("StringValue")
                word = attributes.get("word", {}).get("StringValue")

                collected_messages.append((int(order_num), word))
                logger.info(f"Processing message with order_num: {order_num}, word: {word}")
                print(f"Received message with order_num: {order_num}, word: {word}")

                q = sqs.get_queue_attributes(
                    QueueUrl=sqs_url,
                    AttributeNames=[
                        "ApproximateNumberOfMessages",
                        "ApproximateNumberOfMessagesNotVisible",
                        "ApproximateNumberOfMessagesDelayed",
                    ],
                )["Attributes"]
                print("Updated SQS Queue Attributes:", q)
                logger.info(f"Updated SQS Queue Attributes: {q}")

                sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=message["ReceiptHandle"])
                logger.info(f"Deleted message with order_num: {order_num} from SQS queue.")

        return collected_messages

    # task four: assembling final message by ordering words according to their assigned order
    @task
    def assemble_message(collected_messages: list[tuple[int, str]]):
        logger.info("Assembling final phrase...")
        collected_messages.sort()
        words = [word for (_n, word) in collected_messages]
        final_phrase = " ".join(words)
        print("Final phrase:", final_phrase)
        logger.info(f"Assembled final phrase: {final_phrase}")
        return final_phrase

    # task five: submitting final message to submission SQS queue
    @task
    def submit_message(final_phrase: str, uvaid: str = "wkt7ne", platform: str = "airflow"):
        logger.info("Submitting final phrase...")
        submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
        load_dotenv()
        REGION_NAME = os.getenv("REGION_NAME", "us-east-1")
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

        sqs = boto3.client(
            "sqs",
            region_name=REGION_NAME,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        try:
            response = sqs.send_message(
                QueueUrl=submit_url,
                MessageBody=final_phrase,
                MessageAttributes={
                    "uvaid": {"DataType": "String", "StringValue": uvaid},
                    "phrase": {"DataType": "String", "StringValue": final_phrase},
                    "platform": {"DataType": "String", "StringValue": platform},
                },
            )
            print("Submit response:", response)
            print("Message submitted successfully.")
            logger.info("Submission succeeded.")
        except Exception as e:
            print("Error submitting message:", e)
            logger.error(f"Error submitting message: {e}")
            raise

    # creating a flow with defined tasks 
    sqs_url = fetch_sqs_url()
    _ = get_sqs_attributes(sqs_url)
    collected = build_pairs(sqs_url)
    final_phrase = assemble_message(collected)
    submit_message(final_phrase)
