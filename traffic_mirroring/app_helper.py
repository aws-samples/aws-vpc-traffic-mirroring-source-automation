import logging
import os
import yaml
from botocore.exceptions import ClientError
from dataclasses import dataclass

log = logging.getLogger()
log.setLevel(logging.INFO)

CONFIG_FILE = "config/" + os.environ['AWS_REGION'] + ".yaml"
SUBNET_TAG_KEY = "TargetSubnetId"
START_BACKFILL_TOKEN = "StartToken"
SESSION_NUMBER = 100

@dataclass
class Instance:
    network_interface_id: str
    subnet_id: str
    vpc_id: str
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

    if field_missing('filterId', target_config):
        log.error('Missing filterId')
        return False

    if not field_missing('targetId', target_config):
        return True

    if field_missing('targetInstanceType', target_config):
        log.error('Missing targetInstanceType and targetId not specified')
        return False
    if field_missing('targetInstanceAmi', target_config):
        log.error('Missing targetInstanceAmi and targetId not specified')
        return False
    if not valid_list_field('targetSecurityGroupIds', target_config):
        log.error('Invalid targetSecurityGroupIds and targetId not specified')
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

def create_instance_object(instance_details):
    subnet_id = instance_details["SubnetId"]
    vpc_id = instance_details["VpcId"]
    # Extracting only primary interface
    network_interface_id = instance_details["NetworkInterfaces"][0]["NetworkInterfaceId"]
    tags = parse_instance_tags(instance_details)

    return Instance(network_interface_id, subnet_id, vpc_id, tags)

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
    target_id = get_or_create_target(ec2, instance, config)
    filter_id = config["filterId"]
    network_interface_id = instance.network_interface_id
    log.info("Creating a session with source: %s, target: %s, filter: %s", 
            network_interface_id, target_id, filter_id)

    try:
        response = create_traffic_mirror_session(ec2, network_interface_id, target_id, filter_id)
        log.info("Successfully created a traffic mirror session: %s", response)
    except ClientError as e:
        if e.response['Error']['Code'] == 'TrafficMirrorSourcesPerTargetLimitExceeded':
            log.info("Target %s has reached its limit.", target_id)
            # If a target was not supplied in the config one is created dynamically
            if not using_predefined_target(config):
                target_subnet_id = get_target_subnet_id(instance, config)
                log.info("Creating a new target in %s", target_subnet_id)
                create_target(ec2, target_subnet_id, config, target_id)
                create_session(ec2, instance, config)
            else:
                raise e
        else:
            raise e

def create_traffic_mirror_session(ec2, network_interface_id, target_id, filter_id):
    return ec2.create_traffic_mirror_session(NetworkInterfaceId=network_interface_id,
                                             TrafficMirrorTargetId=target_id,
                                             TrafficMirrorFilterId=filter_id,
                                             SessionNumber=SESSION_NUMBER)

# Gets or creates a valid target to use in a session. If a targetId was specified in the config
# it will be used. If not, an existing target will be looked up using tags. If no target is found, one will be created
def get_or_create_target(ec2, instance, config):
    if using_predefined_target(config):
        targetId = config['targetId']
        log.info('Using provided target: %s', targetId)
        return targetId

    log.info('No target provided in config, determining if one needs to be created')
    target_subnet_id = get_target_subnet_id(instance, config)
    target_tag_filter = create_response_filter(
        "tag:" + SUBNET_TAG_KEY, target_subnet_id)
    response = describe_targets(ec2, target_tag_filter)

    if response["TrafficMirrorTargets"]:
        return response["TrafficMirrorTargets"][0]["TrafficMirrorTargetId"]
    else:
        log.info('No available target found. Creating one')
        return create_target(ec2, target_subnet_id, config)

def get_target_subnet_id(instance, config):
    return config["targetSubnetId"] if not field_missing('targetSubnetId', config) else instance.subnet_id

# Creates a traffic mirror target with the desired configuration and tags it
# The tag is used to find an available target for a subnet. If a target reaches its source per target limit,
# a new target is created and the tags on the previous target is removed.
def create_target(ec2, target_subnet_id, config, existing_target_id = None):
    target_instance = launch_target_instance(ec2, target_subnet_id, config)
    network_interface_id = target_instance["Instances"][0]["NetworkInterfaces"][0]["NetworkInterfaceId"]

    try:
        target_id = create_target_with_tag(ec2, network_interface_id, target_subnet_id)
        remove_subnet_tag(ec2, target_subnet_id, existing_target_id)
        log.info("TrafficMirrorTarget %s created", target_id)
        return target_id
    except Exception as e:
        target_instance_id = target_instance["Instances"][0]["InstanceId"]
        log.info("Failed to create target. Terminating the launched target instance %s", target_instance_id)
        ec2.terminate_instances(InstanceIds=[target_instance_id])
        raise e

def launch_target_instance(ec2, target_subnet_id, config):
    log.info("Launching a new EC2 instance for target")
    return ec2.run_instances(ImageId=config["targetInstanceAmi"],
                            SubnetId=target_subnet_id,
                            InstanceType=config["targetInstanceType"],
                            SecurityGroupIds=config["targetSecurityGroupIds"],
                            MinCount=1, MaxCount=1)

def create_target_with_tag(ec2, network_interface_id, target_subnet_id):
    tag_specifications = {
        "ResourceType": "traffic-mirror-target",
        "Tags": [
            create_tag(SUBNET_TAG_KEY, target_subnet_id),
            create_tag("Name", "TrafficMirroringSourceAutomation")
        ]
    }
    log.info("Creating a traffic mirror target with networkInterfaceId: %s", network_interface_id)
    response = ec2.create_traffic_mirror_target(NetworkInterfaceId=network_interface_id, 
                                                TagSpecifications=[tag_specifications])
    return response["TrafficMirrorTarget"]["TrafficMirrorTargetId"]

def describe_targets(ec2, filters):
    return ec2.describe_traffic_mirror_targets(Filters=[filters])

def remove_subnet_tag(ec2, target_subnet_id, existing_target_id):
    if existing_target_id:
        log.info("Removing tag from %s", existing_target_id)
        ec2.delete_tags(Resources=[existing_target_id], Tags=[create_tag(SUBNET_TAG_KEY, target_subnet_id)])

def create_response_filter(name, value):
	return {"Name": name, "Values": [value]}
	  
def create_tag(key, value):
    return { "Key": key, "Value": value }

# Returns true if the target_config has defined the target to use
def using_predefined_target(target_config):
	return not field_missing('targetId', target_config)
