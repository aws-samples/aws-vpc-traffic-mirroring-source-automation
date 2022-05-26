import boto3
import botocore
import logging
from app_helper import *
from botocore.exceptions import ClientError
from dataclasses import dataclass

log = logging.getLogger()
log.setLevel(logging.INFO)

@dataclass
class SNSConfig:
    topic_arn: str
    next_token: str

SNS_CONFIG = SNSConfig(None, None)
MAX_RESULTS = 200
CONFIG = load_config()

ec2 = boto3.client('ec2')
sns = boto3.client('sns')

# Entry point for Lambda function which parse the instance launch event and handles it based
# on the user provided configuration
def lambda_handler(event, context):
    log.info("Parsing event: %s", event)

    instances = parse_sns_message(event)
    if not instances:
        return

    for instance in instances:
        log.info("Parsed instance: %s", instance)
        if requires_session(ec2, instance):
            try:
                handle_event(ec2, instance, CONFIG)
            except ClientError as e:
                log.error("Failed to setup traffic mirroring for %s due to error: %s",
                          instance.network_interface_id, e)
    
    publish_sns_message()

# Publishes a message to SNS Topic if a next token in available from the describe-instances call   
def publish_sns_message():
    if SNS_CONFIG.next_token:
        publish_message(sns, SNS_CONFIG.topic_arn, SNS_CONFIG.next_token)

# Parses the SNS message and returns a list of Instances
# Uses the "NextToken" in the message to describe instances
def parse_sns_message(event):
    if "Records" not in event:
        log.error("The event type is either invalid or not supported")
        return []

    next_token = event["Records"][0]["Sns"]["MessageAttributes"]["NextToken"]["Value"]
    response = describe_instances(next_token)

    if not response:
        log.info("Finished backfilling existing instances.")
        return []

    instance_list = []
    for reservations in response["Reservations"]:
        #loop instance in reservations (for multi instance reservation)
        for instance_details in reservations["Instances"]:
            instance = create_instance_object(instance_details)
            if instance is not None:
                instance_list.append(instance)

    update_sns_config(event, response)

    return instance_list

def describe_instances(next_token):
    if not next_token or next_token == START_BACKFILL_TOKEN:
        return ec2.describe_instances(MaxResults=MAX_RESULTS)

    return ec2.describe_instances(MaxResults=MAX_RESULTS, NextToken=next_token)

def update_sns_config(event, describe_instances_response):
    global SNS_CONFIG
    SNS_CONFIG.topic_arn = event["Records"][0]["Sns"]["TopicArn"]
    if "NextToken" in describe_instances_response:
        SNS_CONFIG.next_token = describe_instances_response["NextToken"]
    else:
        SNS_CONFIG.next_token = None

