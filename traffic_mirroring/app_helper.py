# This sample, non-production-ready code allows the user to automate setting up of traffic mirroring based on VPCs, subnets, and tags as input.    
# © 2021 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.  
# This AWS Content is provided subject to the terms of the AWS Customer Agreement available at  
# http://aws.amazon.com/agreement or other written agreement between Customer and either
# Amazon Web Services, Inc. or Amazon Web Services EMEA SARL or both.
#
# This is a customized solution based on original AWS solution at https://github.com/aws-samples/aws-vpc-traffic-mirroring-source-automation
# 

import logging
import os
import sys
import yaml
from botocore.exceptions import ClientError
from dataclasses import dataclass

log = logging.getLogger()
log.setLevel(logging.INFO)

CONFIG_FILE = "config/" + os.environ['AWS_REGION'] + ".yaml"
SUBNET_TAG_KEY = "TargetSubnetId"
START_BACKFILL_TOKEN = "StartToken"
SESSION_NUMBER = 100
FILTER_TAG_KEY = "server_type" # Instance tag 'server_type' is required; acceptable values are 'web', 'app' and 'db'

@dataclass
class Instance:
    network_interface_id: str
    subnet_id: str
    vpc_id: str
    az_id: str # customized to capture AZ ID as we are assigning traffic mirror source and target in the same AZ based on AZ ID.
    tags: list

def load_config():
    with open(CONFIG_FILE, 'r') as f:
        config = yaml.safe_load(f)
        log.info("Loaded config: %s", config)
        if not valid_config(config):
            raise ValueError('Invalid config')
        log.info('Config is valid')
        return config

def valid_config(config):
    if not valid_source_type('vpcs', 'vpcId', config):
        return False
    if not valid_source_type('subnets', 'subnetId', config):
        return False
    if not valid_source_type('tags', 'tagList', config):
        return False

    return True

def valid_source_type(source_type, source_identifier, config):
    if source_type in config:
        log.info('Validating ' + source_type)
        for target_config in config[source_type]:
            if not valid_target_config(source_identifier, target_config):
                return False
            if (source_type == 'tags') and not valid_tag_based_target_config(target_config):
                return False

    return True

def valid_target_config(source, target_config):
    if field_missing(source, target_config):
        log.error('Missing ' + source)
        return False

    return True

def valid_tag_based_target_config(target_config):
    tags = target_config['tagList']
    if type(tags) is not list:
        log.error('Missing tags in tagList')
        return False

    for tag in tags:
        if field_missing('Key', tag):
            log.error('Missing tag key')
            return False

    return True

def valid_list_field(field, config):
    if field not in config:
        return False

    value = config[field]
    if type(value) is not list:
        return False
    if len(value) == 0:
        return False
    for item in value:
        if empty(item):
            return False

    return True

def field_missing(field, config):
    if field not in config:
        return True
    return empty(config[field])

def empty(value):
    return not value or value == '' or value == '<INSERT_VALUE>'

def check_key_and_value(dictionary, key, value):
    return key in dictionary and dictionary[key] == value

# Publishes a message to the specified Topic
def publish_message(sns_client, sns_topic_arn, next_token):
    log.info("Publishing message to SNS topic %s with next_token %s", sns_topic_arn, next_token)
    message_attribute = {"NextToken": {"DataType": "String", "StringValue": next_token}}
    sns_client.publish(TopicArn=sns_topic_arn,
                       Message="Backfill existing instances", MessageAttributes=message_attribute)

def create_instance_object(instance_details,subnet_details):
    subnet_id = instance_details["SubnetId"]
    vpc_id = instance_details["VpcId"]
    # Extracting only primary interface
    network_interface_id = instance_details["NetworkInterfaces"][0]["NetworkInterfaceId"]
    tags = parse_instance_tags(instance_details)
    
    # customized to capture AZ ID as we are assigning traffic mirror source and target in the same AZ based on AZ ID.
    az_id = subnet_details['Subnets'][0]['AvailabilityZoneId']

    return Instance(network_interface_id, subnet_id, vpc_id, az_id, tags)

# Extract instance tags if available
def parse_instance_tags(instance_details):
    if "Tags" in instance_details:
        return instance_details["Tags"]
    elif "tags" in instance_details:
        return instance_details["tags"]
    return None

# Validate if the instance needs to be setup with Traffic Mirroring
def requires_session(ec2, instance):
    target_filter = {"Name": "network-interface-id", "Values": [instance.network_interface_id]}
    response = describe_targets(ec2, target_filter)

    if response["TrafficMirrorTargets"]:
        log.info("A target %s exists for interface %s. Skipping setting up traffic mirroring session",
                 response["TrafficMirrorTargets"][0]["TrafficMirrorTargetId"],
                 instance.network_interface_id)
        return False
    return True

# Handler for an event. 
# Subnet configuration is given a priority over VPC configuration
# Tag configuration is given a priority over subnet configuration
def handle_event(ec2, instance, config):
    if instance.tags and "tags" in config:
        for tag_config in config["tags"]:
            matched_tags = find_matching_tags(instance.tags, tag_config["tagList"])
            if matched_tags:
                log.info("Instance's tags matches the tracked tags %s", matched_tags)
                create_session(ec2, instance, tag_config)
                return

    if "subnets" in config:
        for subnet_config in config["subnets"]:
            if instance.subnet_id == subnet_config["subnetId"]:
                log.info("Instance's subnet matches the tracked subnet %s", instance.subnet_id)
                create_session(ec2, instance, subnet_config)
                return

    if "vpcs" in config:
        for vpc_config in config["vpcs"]:
            if instance.vpc_id == vpc_config["vpcId"]:
                log.info("Instance's vpc matches the tracked vpc %s", instance.vpc_id)
                create_session(ec2, instance, vpc_config)
                return
    
    log.info("Instance's properties does not match the tracked tags/subnets/VPCs")

# Returns a list of tags present in both config and describe-instances response
def find_matching_tags(instance_tags, config_tags):
    return [tags for tags in instance_tags if tags in config_tags]

# Creates a Traffic Mirroring Session
def create_session(ec2, instance, config):
    target_id = get_target(instance, config)
    filter_id = get_filter(instance, config)
    network_interface_id = instance.network_interface_id
    if target_id == 'invalid_target_id' or filter_id == 'invalid_filter_id':
        log.error("Invalid TargetID or filterID found in config. Please review config yaml file.")
    else:
        log.info("Creating a session with source: %s, target: %s, filter: %s", 
                network_interface_id, target_id, filter_id)
    
        try:
            response = create_traffic_mirror_session(ec2, network_interface_id, target_id, filter_id)
            log.info("Successfully created a traffic mirror session: %s", response)
        except ClientError as e:
            if e.response['Error']['Code'] == 'TrafficMirrorSourcesPerTargetLimitExceeded':
                log.info("Target %s has reached its limit.", target_id)
                raise e
            else:
                raise e

def create_traffic_mirror_session(ec2, network_interface_id, target_id, filter_id):
    return ec2.create_traffic_mirror_session(NetworkInterfaceId=network_interface_id,
                                             TrafficMirrorTargetId=target_id,
                                             TrafficMirrorFilterId=filter_id,
                                             SessionNumber=SESSION_NUMBER)

# Gets a valid target to use in a session. If a targetId was specified in the config
# it will be used. If not, it will error out with details in the logfile.
def get_target(instance, config):
    # customized to retrieve targetID from input config file based on AZ ID of the instance
    azid = instance.az_id
    aznum = azid[-3:]    
    target_name ='targetID-' + aznum

    if field_missing(target_name, config):
        log.error("TargetID %s not found in config. Please review config yaml file and update targetID.", target_name)
        return "invalid_target_id"
    else:
        targetId = config[target_name]
        log.info('Using provided target: %s', targetId)
        return targetId

def get_filter(instance, config):
    for tag in instance.tags:
        if tag['Key'] == FILTER_TAG_KEY:
            if tag['Value'] in ('web','app','db'):
                filter_name = 'filterID-' + tag['Value']
                if field_missing(filter_name, config):
                    log.error("FilterID %s not found in config. Please review config yaml file and update approproate filterID.",filter_name)
                    return "invalid_filter_id"
                else:
                    filterId = config[filter_name]
                    log.info('Using provided filter: %s', filterId)
                    return filterId
            else:
                log.error("Value for instance tag %s is invalid. Acceptable values are 'web', 'app' and 'db'.",FILTER_TAG_KEY)
                return "invalid_filter_id"

    log.error("Tag %s is missing. Please add instance tag %s.",FILTER_TAG_KEY,FILTER_TAG_KEY)
    return "invalid_filter_id"

def get_target_subnet_id(instance, config):
    return config["targetSubnetId"] if not field_missing('targetSubnetId', config) else instance.subnet_id

def describe_targets(ec2, filters):
    return ec2.describe_traffic_mirror_targets(Filters=[filters])

def create_response_filter(name, value):
	return {"Name": name, "Values": [value]}