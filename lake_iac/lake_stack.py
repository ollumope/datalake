from asyncio.log import logger
import logging
from aws_cdk import (
    # Duration,
    Stack,
    # aws_sqs as sqs,
)
from constructs import Construct
from lake_iac.bucket.s3_bucket import CreateBucket
from lake_iac.iam.iam import CreatePermission
from lake_iac.glue.glue_database import GlueArtifacts
from lake_iac.glue.glue_job import GlueJobs
from lake_iac.glue.glue_crawler import GlueCrawler
from lake_iac.lambda_fn.lambda_fn import LambdaFunction

class LakeStack(Stack):
    '''Resources definition'''

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        aws_region:str,
        aws_account_id:str,
        logging:logging.Logger,
        **kwargs) -> None:

        super().__init__(scope, construct_id, **kwargs)
        role_name = 'AWSGlueServiceRole-lake-execute'
        role_glue = CreatePermission(self, role_name,
                                     'role for glue execution',
                                     'glue.amazonaws.com',
                                     aws_account_id, logging)
        role_lambda = CreatePermission(self, 'role-lake-exec-lambda',
                                     'role for lambda execution',
                                     'lambda.amazonaws.com',
                                     aws_account_id, logging)
        role_arn = f'arn:aws:iam::{aws_account_id}:role/service-role/{role_name}'

        bucket_raw = CreateBucket(self, identifier='olmp-lake-raw')
        bucket_stage = CreateBucket(self, identifier='olmp-lake-stage')

        database = GlueArtifacts(self, id='glue-database', aws_account_id=aws_account_id,
                                 aws_region=aws_region, bucket=bucket_stage.bucket_name, logging=logging)
        role_viejo = 'arn:aws:iam::081900802975:role/service-role/AWSGlueServiceRole-crawlers'

        glue_job = GlueJobs(self, id='job_uber_movements',
                            role=role_arn,
                            bucket=bucket_stage.bucket_name)

        GlueCrawler(self, id='crawler_uber_movements', role=role_arn,
                    db_name=database.db_name, tb_name=database.tb_name,
                    desc='Catalog uber tables', bucket=bucket_stage.bucket_name)

        LambdaFunction(self, identifier='lambda-function-initialize',
                       job_name=glue_job.job_name, role_arn=role_lambda.role_arn,
                       logger=logging, event_id='ev-uber-movements',
                        bucket_raw=bucket_raw.bucket_name,
                        bucket_stage=bucket_stage.bucket_name,
                        db_name=database.db_name,
                        tb_name=database.tb_name)
