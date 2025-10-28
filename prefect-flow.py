# prefect flow goes here
import requests
import boto3
import os
from dotenv import load_dotenv
import time, random
from prefect import flow, task, get_run_logger



@task
# fetch the SQS URL via the provided API endpoint, request the url using the `post` method
def fetch_sqs_url():
    logger = get_run_logger()

    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/wkt7ne"

    # using the post method to request the url
    payload = requests.post(url).json()

    # extract the SQS URL from the response payload by locating the 'sqs_url' key
    sqs_url = payload['sqs_url']

    logger.info(f"Fetched SQS URL: {sqs_url}")
    # return the url as a string to be used in subsequent tasks
    return sqs_url

@task
# get SQS attributes
def get_sqs_attributes(sqs_url):

    # load environment variables for AWS credentials and region
    logger = get_run_logger()
    load_dotenv()
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    REGION_NAME = os.getenv("REGION_NAME", "us-east-1")
    logger.info("Loaded AWS credentials and region from environment variables.")

    # create an SQS client using boto3 with the loaded credentials
    sqs = boto3.client('sqs',
                    region_name = REGION_NAME,
                    aws_access_key_id = AWS_ACCESS_KEY_ID,
                    aws_secret_access_key = AWS_SECRET_ACCESS_KEY
    )
    logger.info("Created SQS client using boto3.")

    # identify the attributes to fetch from the SQS queue
    attributes = ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"]
    response = sqs.get_queue_attributes(
        QueueUrl=sqs_url,
        AttributeNames=attributes
    )
    logger.info("Fetched SQS queue attributes.")

    # print the fetched attributes for verification and monitoring purposes
    attribute_content = response['Attributes']
    print("SQS Queue Attributes:", attribute_content)
    logger.info(f"Fetched SQS Queue Attributes: {attribute_content}")
    return sqs


def build_pairs(sqs, sqs_url, expected_count = 21):
    logger = get_run_logger()
    collected_messages = []
    # collect messages until the expected count is reached
    while len(collected_messages) < expected_count:
        try: 
            response = sqs.receive_message(
                QueueUrl=sqs_url,
                # fetch up to 10 messages at a time
                MaxNumberOfMessages=10,
                # retrieve all message attributes
                MessageAttributeNames=['All'],
                # short polling wait time
                WaitTimeSeconds=5
            )
            logger.info("Received messages from SQS queue.")
        except Exception as e:
            logger.error(f"Error receiving messages: {e}")
            print("Error receiving messages:", e)
            continue

        # process each received message
        for message in response.get("Messages", []):
            attributes = message.get("MessageAttributes", {})
            # extract order number and word from message attributes
            order_num = attributes.get("order_no", {}).get("StringValue")
            word = attributes.get("word", {}).get("StringValue")

            # store the extracted pair in the collected messages list
            collected_messages.append((int(order_num), word))
            logger.info(f"Processing message with order_num: {order_num}, word: {word}")
            print(f"Received message with order_num: {order_num}, word: {word}")

            # update and print SQS attributes after processing each message (for monitoring)
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

            # delete the processed message from the SQS queue to prevent reprocessing
            sqs.delete_message(
                QueueUrl=sqs_url,
                ReceiptHandle=message['ReceiptHandle']
            )
            logger.info(f"Deleted message with order_num: {order_num} from SQS queue.")
    
    return collected_messages
@task
# assemble the final message from collected word-order pairs
def assemble_message(collected_messages):
    logger = get_run_logger()
    # sort the collected messages by order number
    collected_messages.sort()
    logger.info("Sorted collected messages by order number.")

    # extract words in the correct order and join them into a single phrase
    words = [word for (order_num, word) in collected_messages]
    final_phrase = " ".join(words)
    logger.info(f"Assembled final phrase from collected words: {final_phrase}")
    print("Final phrase:", final_phrase)
    return final_phrase
    
@task
# submit the final message to the designated SQS queue
def submit_message(uvaid, final_phrase, platform):
    logger = get_run_logger()
    submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    load_dotenv()
    REGION_NAME = os.getenv("REGION_NAME", "us-east-1")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    logger.info("Loaded AWS credentials and region from environment variables for submission.")

    # define a new client to avoid prefect errors 
    sqs = boto3.client(
        "sqs",
        region_name=REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    logger.info("Created SQS client for submission using boto3.")

    try:
        # send the final message to the submission SQS queue with required attributes
        response = sqs.send_message(
            QueueUrl=submit_url,
            MessageBody=final_phrase,
            MessageAttributes={
                "uvaid": {
                    "DataType": "String",
                    "StringValue": uvaid
                },
                "phrase": {
                    "DataType": "String",
                    "StringValue": final_phrase
                },
                "platform": {
                    "DataType": "String",
                    "StringValue": platform
                }
            })
        logger.info("Submitted final message to submission SQS queue.")
        print("Submit response:", response)
        print("Message submitted successfully.")
    except Exception as e:
        logger.error(f"Error submitting message: {e}")
        print("Error submitting message:", e)

@flow
# run the prefect flow 
def main():
    sqs_url = fetch_sqs_url()
    sqs = get_sqs_attributes(sqs_url)
    collected_messages = build_pairs(sqs, sqs_url)
    final_phrase = assemble_message(collected_messages)
    submit_message(uvaid="wkt7ne", final_phrase=final_phrase, platform="prefect")
if __name__ == "__main__":
    main()


             