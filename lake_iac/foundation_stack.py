import logging
from unicodedata import name
from aws_cdk import (
    Stack
)
from constructs import Construct
from lake_iac.bucket.s3_bucket import CreateBucket
from lake_iac.iam.iam import CreatePermission
from lake_iac.glue.glue_database import GlueArtifacts


class FoundationStack(Stack):
    '''Resources definition for fundations resource:
        Buckets and iam roles'''

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        aws_account_id:str,
        aws_region:str,
        logging:logging.Logger,
        **kwargs) -> None:

        super().__init__(scope, construct_id, **kwargs)
        self.bucket_raw = CreateBucket(self, identifier='olmp-lake-raw')
        self.bucket_stage = CreateBucket(self, identifier='olmp-lake-stage')
        self.role_glue = CreatePermission(self, 'AWSGlueServiceRole-lake-execute',
                                     'role for glue execution',
                                     'glue.amazonaws.com',
                                     aws_account_id, logging)

        self.role_lambda = CreatePermission(self, 'role-lake-exec-lambda',
                                     'role for lambda execution',
                                     'lambda.amazonaws.com',
                                     aws_account_id, logging)
        self.database = GlueArtifacts(self, id='glue-database', aws_account_id=aws_account_id,
                                 aws_region=aws_region, bucket=self.bucket_stage.bucket_name, logging=logging)
