#!/usr/bin/env python3
import os
import aws_cdk as cdk
import logging
from lake_iac.lake_stack import LakeStack
import logging as log
logger = log.getLogger()

env=cdk.Environment(
    account=os.environ["CDK_DEFAULT_ACCOUNT"],
    region=os.environ["CDK_DEFAULT_REGION"])

app = cdk.App()
aws_region = cdk.Aws.REGION
aws_account_id = cdk.Aws.ACCOUNT_ID

logging.info('Region: %s', aws_region)
logging.info('Account ID: %s', aws_account_id)

LakeStack(
    app,
    "LakeStack",
    env=env,
    aws_region=aws_region,
    aws_account_id=aws_account_id,
    logging = logger
    )

app.synth()
