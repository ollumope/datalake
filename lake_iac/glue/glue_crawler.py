"""
glue_crawler:
Includes the process of catalog database tables
Classes:
    GlueCrawler
"""
import logging
import json
import os
from aws_cdk import aws_glue as glue
from constructs import Construct

class GlueCrawler(Construct):
    def __init__(
        self,
        scope: Construct, 
        id:str, role:str, 
        db_name:str, 
        tb_name:str, 
        desc:str, 
        bucket:str,
        **kwargs) -> None:

        super().__init__(scope, id, **kwargs)
        self.create_crawler(id, role, db_name, tb_name, desc, bucket)

    def create_crawler(self, id, role, db_name, tb_name, desc, bucket):
        cfn_crawler = glue.CfnCrawler(
            self, id=id,
            role=role,
            database_name=db_name,
            description=desc,
            name=id,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(
                connection_name="s3_connection",
                event_queue_arn="eventQueueArn",
                path=bucket,
            )]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior="LOG",
                update_behavior="UPDATE_IN_DATABASE"
            ),
        )
        return cfn_crawler

