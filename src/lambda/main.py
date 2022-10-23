'''Lambda function to execute glue job'''
import json
import os
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)
client = boto3.client('glue')

# Env variable
glueJobName = os.environ['GLUE_JOB_NAME']
bucket_raw = os.environ['BUCKET_RAW']
bucket_stage = os.environ['BUCKET_STAGE']
dabase_name = os.environ['DATABASE_NAME']
table_name = os.environ['TABLE_NAME']

# Define Lambda function
def lambda_handler(event, context):
    logger.info('Initializa by event')
    response = client.start_job_run(
        JobName = glueJobName,
        Arguments = {
                 '--aws_bucket_source': bucket_raw,
                 '--aws_bucket_target': bucket_stage,
                 '--database': dabase_name,
                 '--table': table_name}
        )
    logger.info('Started....' + glueJobName)
    return response
