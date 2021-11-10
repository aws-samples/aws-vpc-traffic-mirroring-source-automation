# This sample, non-production-ready code allows the user to automate setting up of traffic mirroring based on VPCs, subnets, and tags as input.    
# © 2021 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.  
# This AWS Content is provided subject to the terms of the AWS Customer Agreement available at  
# http://aws.amazon.com/agreement or other written agreement between Customer and either
# Amazon Web Services, Inc. or Amazon Web Services EMEA SARL or both.
#
# This is a customized solution based on original AWS solution at https://github.com/aws-samples/aws-vpc-traffic-mirroring-source-automation

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

# customized to also retrieve instance AZ ID 

    instance_list = []
    for reservation in response["Reservations"]:
        for inst in reservation["Instances"]:
            instance_details = inst
            subnet_id = instance_details["SubnetId"]
            subnet_response = ec2.describe_subnets(SubnetIds=[subnet_id])

            instance = create_instance_object(instance_details,subnet_response)
            instance_list.append(instance)

    update_sns_config(event, response)

    return instance_list

def describe_instances(next_token):
# customized to check for instances that are in 'running' state 
    if not next_token or next_token == START_BACKFILL_TOKEN:
        return ec2.describe_instances(Filters=[{'Name': 'instance-state-name','Values': ['running']}],MaxResults=MAX_RESULTS)

    return ec2.describe_instances(Filters=[{'Name': 'instance-state-name','Values': ['running']}],MaxResults=MAX_RESULTS, NextToken=next_token)

def update_sns_config(event, describe_instances_response):
    global SNS_CONFIG
    SNS_CONFIG.topic_arn = event["Records"][0]["Sns"]["TopicArn"]
    if "NextToken" in describe_instances_response:
        SNS_CONFIG.next_token = describe_instances_response["NextToken"]
    else:
        SNS_CONFIG.next_token = None

