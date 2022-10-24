import logging
import json
import aws_cdk as cdk
import logging
from constructs import Construct

class CreateBucket(Construct):
    def __init__(
        self,
        scope:Construct,
        identifier:str,
        **kwargs):
        super().__init__(scope, identifier, **kwargs)
        self.bucket = self.create_bucket(identifier)
        self.bucket_name = identifier

    def create_bucket(self, id):
        '''Function to create a aws S3 bucket
        input:
            id: AWS S3 bucket name and identifier
        output:
            bucket definition'''
        logging.info('Creating s3 bucket ', id)
        bucket = cdk.aws_s3.Bucket(
            self,
            id=id,
            bucket_name=id,
            block_public_access=cdk.aws_s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True
        )
        cdk.Tags.of(scope=bucket).add(
                    "service", "storage"
                    )
        return bucket
    
    def _bucket(self):
        return self.bucket

