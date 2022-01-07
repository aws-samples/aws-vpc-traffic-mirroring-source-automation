# This sample, non-production-ready code allows the user to automate setting up of traffic mirroring based on VPCs, subnets, and tags as input.    
# © 2021 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.  
# This AWS Content is provided subject to the terms of the AWS Customer Agreement available at  
# http://aws.amazon.com/agreement or other written agreement between Customer and either
# Amazon Web Services, Inc. or Amazon Web Services EMEA SARL or both.

import boto3
import botocore
import logging
from app_helper import *
from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

CONFIG = load_config()

ec2 = boto3.client('ec2')
sns = boto3.client('sns')

# Entry point for Lambda function which parse the instance launch event and handles it based 
# on the user provided configuration
def lambda_handler(event, context):
    log.info("Parsing event: %s", event)
    
    instance = parse_event(event)
    if not instance:
        log.warn("The event type is either invalid or not supported")
        return

    log.info("Parsed instance: %s", instance)
    if requires_session(ec2, instance):
        try:
            handle_event(ec2, instance, CONFIG)
        except ClientError as e:
            log.error("Failed to setup traffic mirroring for %s due to error: %s",
                      instance.network_interface_id, e)

# Parse a custom "Instance" object from CloudWatch event
def parse_event(event):
    if check_key_and_value(event, "detail-type", "EC2 Instance State-change Notification"):
        return parse_instance_launch_event(event)
    if check_key_and_value(event, "detail-type", "GuardDuty Finding"):
        return parse_guardduty_event(event)
    else:
        return None

# Parse a custom "Instance" object from an Instance Launch event
def parse_instance_launch_event(event):
    instance_id = event["detail"]["instance-id"]
    response = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]

    # customized to also retrieve instance AZ ID
    subnet_id = response["SubnetId"]
    subnet_response = ec2.describe_subnets(SubnetIds=[subnet_id])

    return create_instance_object(response, subnet_response)

# Parse a custom "Instance" from a GaurdDuty Finding event
def parse_guardduty_event(event):
    if "resource" in event["detail"] and event["detail"]["resource"]["resourceType"] == "Instance":
        instance_details = event["detail"]["resource"]["instanceDetails"]

        # Get AZID
        instance_id = instance_details["instanceId"]
        response = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]
        subnet_id = response["SubnetId"]
        subnet_response = ec2.describe_subnets(SubnetIds=[subnet_id])
        az_id = subnet_response['Subnets'][0]['AvailabilityZoneId']

        # Extracting only primary interface
        network_interface_id = instance_details["networkInterfaces"][0]["networkInterfaceId"]
        subnet_id = instance_details["networkInterfaces"][0]["subnetId"]
        vpc_id = instance_details["networkInterfaces"][0]["vpcId"]
        tags = parse_instance_tags(instance_details)

        return Instance(network_interface_id, subnet_id, vpc_id, az_id, tags)
    return None
