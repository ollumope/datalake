"""
Glue:
Includes the database and definition for datalake storage
Classes:
    GlueJobs
"""
import logging
import json
import os
import boto3
from aws_cdk import aws_glue as glue
from constructs import Construct

class GlueJobs(Construct):
    def __init__(self, scope: Construct, id: str,
                 role: str,
                 bucket:str,
                 **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        base_path = os.getcwd()
        script_loc_path = f"{base_path}/src/glue_jobs/main.py"
        s3_client = boto3.client('s3')
        s3_client.upload_file(
            Filename=script_loc_path,
            Bucket=bucket,
            Key='glue/artifacts/main.py')
        script_loc_path_s3 = f's3://{bucket}/glue/artifacts/main.py'
        job = self.create_job(id, script_loc_path_s3, role)
        self.job_name = job.name

    def create_job(self, id, script_loc, role):
        cfn_job = glue.CfnJob(
            self, 
            name=id,
            id=id,
            command=glue.CfnJob.JobCommandProperty(
                name='pythonshell',
                python_version="3.9",
                script_location=script_loc
            ),
            role=role)
        return cfn_job
