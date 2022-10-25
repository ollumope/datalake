import logging
import os

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
        aws_account_id:str,
        logging:logging.Logger,
        **kwargs) -> None:
        '''
        AWS CDK Refuses to execute the stack provisioning in the specified order generating errors on stack creation.
        To avoid the creation issue, a workaround was implemented to gradualy provision the resources so Cloudformation
        always has the dependant resources.
        '''
        super().__init__(scope, construct_id, **kwargs)
        self.logging = logging
        
        step = os.environ.get('STEP')

        if step == 'BUCKETS':
            self.buckets()

        if step == 'ROLES':
            self.buckets()
            self.roles(aws_account_id=aws_account_id)
            

        if step == 'DB':
            self.buckets()
            self.roles(aws_account_id)
            self.database(aws_account_id)
            
        if step == 'TABLE':
            self.buckets()
            self.roles(aws_account_id)
            self.database(aws_account_id)
            self.table(aws_account_id)

        if step == 'JOB':
            self.buckets()
            self.roles(aws_account_id)
            self.database(aws_account_id)
            self.table(aws_account_id)
            self.job()
        
        if step == 'ALL':
            self.buckets()
            self.roles(aws_account_id)
            self.database(aws_account_id)
            self.table(aws_account_id)
            self.job()
            self.lambda_fn()




    def roles(self, aws_account_id): 
        self.role_glue = CreatePermission(self, 'AWSGlueServiceRole-lake-execute',
                                     'role for glue execution',
                                     'glue.amazonaws.com',
                                     aws_account_id, self.logging)

        self.role_lambda = CreatePermission(self, 'role-lake-exec-lambda',
                                     'role for lambda execution',
                                     'lambda.amazonaws.com',
                                     aws_account_id, self.logging)

    def buckets(self):
        self.bucket_raw = CreateBucket(self, identifier='olmp-lake-raw')
        self.bucket_stage = CreateBucket(self, identifier='olmp-lake-stage')

    def database(self, aws_account_id):
        self.gluedatabase = CreateGlueDataBase(self, id='glue-database', aws_account_id=aws_account_id,
                                 bucket=self.bucket_stage.bucket_name,
                                 db_name='db_uber',
                                 logging=self.logging)

    def table(self, aws_account_id):
        self.gluetable = CreateGlueTable(self, id='glue-table', aws_account_id=aws_account_id,
                                 bucket=self.bucket_stage.bucket_name,
                                 tb_name="tb_uber_movements", db_name=self.gluedatabase.db_name,
                                 logging=self.logging)

    def job(self):
        self.glue_job = GlueJobs(self, id='job_uber_movements',
                            role=self.role_glue.role_arn,
                            bucket=self.bucket_stage.bucket_name)
    
    def lambda_fn(self):
        LambdaFunction(self, identifier='lambda-function-initialize',
                       job_name=self.glue_job.job_name, role_arn=self.role_lambda.role_arn,
                       logger=logging,
                        bucket_raw=self.bucket_raw,
                        bucket_stage=self.bucket_stage.bucket_name,
                        db_name=self.gluedatabase.db_name,
                        tb_name=self.gluetable.tb_name)