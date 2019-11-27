import boto3
import botocore
import logging
from app_helper import publish_message, check_key_and_value, START_BACKFILL_TOKEN
from crhelper import CfnResource

log = logging.getLogger()
log.setLevel(logging.INFO)

sns = boto3.client('sns')
cfn_helper = CfnResource()

@cfn_helper.create
def start_backfill(event, _):
    log.info("Initiating setting up traffic mirroring for existing instances")
    sns_topic_arn = event["ResourceProperties"]["SNSTopicArn"]
    publish_message(sns, sns_topic_arn, START_BACKFILL_TOKEN)

@cfn_helper.update
@cfn_helper.delete
def no_op(_, __):
    pass

# Entry point for Lambda function to handle operations on custom cfn resource
def lambda_handler(event, context):
    if check_key_and_value(event, "ResourceType", "Custom::AppConfiguration"):
        cfn_helper(event, context)
