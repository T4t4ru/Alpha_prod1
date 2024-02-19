import json
import logging
import os
import signal
import sys
import time
from base64 import b64decode

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)
log = logger.info

RAW_QUEUE_NAME = "alphaprod_sku_raw"
"""
Lambda function on AWS.
Must be specified using CLI or using specific boto3 syntax. I recommend you to be lazy and use the CLI method 
(or any other method ignoring automatic setup through code).
You can read about it and get details of setting up in docs: https://docs.aws.amazon.com/lambda/latest/dg/urls-configuration.html
"""


def lambda_handler(event, context=None):
    log(f'raw_input\t{event["body"]}')

    # JWT_KEY must be set in environment variables on AWS
    # (details: https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html)
    # The main goal is to set a unique JWT_KEY for clients to make this accessible for them

    if not ("headers" in event and "x-auth" in event['headers']):
        return {"statusCode": 401,
                "body": "You're doing something wrong. Try to specify headers. Maybe it'll help you a bit."}

    if event["headers"]["x-auth"] != os.environ.get("JWT_KEY"):
        return {
            "statusCode": 401,
            "body": "You're doing something wrong. Try to specify headers. Maybe it'll help you a bit.",
        }

    j = json.loads(event["body"])
    log(f"Decoded payload: {j}")

    sqs = boto3.resource(
        "sqs",
        region_name="eu-central-1",
        aws_access_key_id=os.environ.get("ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("SECRET_ACCESS_KEY"),
    )

    # If you want you can rename it or even use os.environ. Information about queue management
    # can be read for example here: https://plainenglish.io/blog/triggering-lambda-to-send-messages-to-sqs
    queue = sqs.create_queue(
        QueueName=os.environ.get("RAW_QUEUE_NAME") or RAW_QUEUE_NAME
    )
    queue.send_message(MessageBody=j)

    result_queue = sqs.get_queue_by_name(QueueName="alphaprod_sku_results")

    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
    response = result_queue.receive_messages(
        MessageAttributeNames=["All"], MaxNumberOfMessages=100, WaitTimeSeconds=30
    )

    messages = response["Messages"]
    receipt_handle = messages["ReceiptHandle"]

    sqs.delete_message(ReceiptHandle=receipt_handle)

    return {
        "statusCode": 200,
        "body": json.dumps(messages),
    }


"""
Got it from example: https://github.com/aws-samples/graceful-shutdown-with-aws-lambda/blob/main/python-demo/hello_world/app.py
Don't really know how it works but I'll leave this here just for sure.
"""


def exit_gracefully(signum, frame):
    print("[runtime] SIGTERM received")

    print("[runtime] cleaning up")
    # perform actual clean up work here.
    time.sleep(0.2)

    print("[runtime] exiting")
    sys.exit(0)

    signal.signal(signal.SIGTERM, exit_gracefully)

# Don't commit code after this comment, please
if __name__ == "__main__":
    event = json.load(open('example.json'))[0]
    lambda_handler(event)
