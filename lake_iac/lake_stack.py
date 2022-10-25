import logging
from aws_cdk import (
    Stack
)
from constructs import Construct
from lake_iac.bucket.s3_bucket import CreateBucket
from lake_iac.iam.iam import CreatePermission
from lake_iac.glue.glue_database import CreateGlueDataBase
from lake_iac.glue.glue_table import CreateGlueTable
from lake_iac.glue.glue_job import GlueJobs
from lake_iac.lambda_fn.lambda_fn import LambdaFunction

class LakeStack(Stack):
    '''Class with aws resources definition'''

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        aws_region:str,
        aws_account_id:str,
        logging:logging.Logger,
        **kwargs) -> None:

        super().__init__(scope, construct_id, **kwargs)

        bucket_raw = CreateBucket(self, identifier='olmp-lake-raw')
        bucket_stage = CreateBucket(self, identifier='olmp-lake-stage')
        role_glue = CreatePermission(self, 'AWSGlueServiceRole-lake-execute',
                                     'role for glue execution',
                                     'glue.amazonaws.com',
                                     aws_account_id, logging)

        role_lambda = CreatePermission(self, 'role-lake-exec-lambda',
                                     'role for lambda execution',
                                     'lambda.amazonaws.com',
                                     aws_account_id, logging)

        gluedatabase = CreateGlueDataBase(self, id='glue-database', aws_account_id=aws_account_id,
                                 bucket=bucket_stage.bucket_name,
                                 db_name='db_uber',
                                 logging=logging)
        
        gluetable = CreateGlueTable(self, id='glue-table', aws_account_id=aws_account_id,
                                 bucket=bucket_stage.bucket_name,
                                 tb_name="tb_uber_movements", db_name=gluedatabase.db_name,
                                 logging=logging)

        glue_job = GlueJobs(self, id='job_uber_movements',
                            role=role_glue.role_arn,
                            bucket=bucket_stage.bucket_name)

        LambdaFunction(self, identifier='lambda-function-initialize',
                       job_name=glue_job.job_name, role_arn=role_lambda.role_arn,
                       logger=logging,
                        bucket_raw=bucket_raw,
                        bucket_stage=bucket_stage.bucket_name,
                        db_name=gluedatabase.db_name,
                        tb_name=gluetable.tb_name)
