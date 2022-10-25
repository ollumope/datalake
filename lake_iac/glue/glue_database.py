import logging
from aws_cdk import aws_glue as glue
import aws_cdk.aws_s3 as aws_s3
from constructs import Construct

class CreateGlueDataBase(Construct):
    # Create AWS Glue databases & tables according to their definitions
    def __init__(self, scope: Construct, id: str,
                 aws_account_id: str,
                 db_name:str,
                 bucket: str,
                 logging:logging.Logger, **kwargs):
        super().__init__(scope, id, **kwargs)
        self.aws_account_id = aws_account_id
        self.db_name = db_name
        logging.info(f"Creating '{self.db_name}' database")
        self.database = self.create_db(self.db_name, self.aws_account_id, bucket)

    def create_db(self, db_name, account_id, stage_bucket):
        '''
        Create Glue database
        input:
            db_name: Name assign to the database
            account_id: Account where the database will be create
            stage_bucket: Bucket name where the data from database are store
        input:
            cfn_database: Database definition
        '''
        cfn_database = glue.CfnDatabase(self,
            id=db_name,
            catalog_id=account_id,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                description="Database for store uber movements",
                location_uri=stage_bucket,
                name=db_name
            ))
        return cfn_database
