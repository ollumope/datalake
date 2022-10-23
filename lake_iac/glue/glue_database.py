"""
Glue:
Includes the database and definition for datalake storage
Classes:
    GlueArtifacts
"""

import logging
import json
import os
from aws_cdk import aws_glue as glue
import aws_cdk.aws_s3 as aws_s3
from constructs import Construct

"""
Glue Class for tables
Parameters:
        scope (Construct): Construct
        id (str): Class ID
        aws_account_id (str): Account ID
        aws_region (str): AWS Region
        bucket (str) : Location bucket
"""


class GlueArtifacts(Construct):
    # Create AWS Glue databases & tables according to their definitions
    def __init__(self, scope: Construct, id: str,
                 aws_account_id: str,
                 aws_region: str, bucket: str,
                 logging:logging.Logger, **kwargs):
        super().__init__(scope, id, **kwargs)
        self.aws_account_id = aws_account_id
        self.aws_region = aws_region
        self.tb_name = "tb_uber_movements"
        self.db_name = 'db_uber'
        base_path = os.getcwd()

        glue_catalog_path = f"{base_path}/lake_iac/glue/"
        file_path = os.path.normpath(
            f"{glue_catalog_path}/table_definition.json"
            )
        logging.info(f"Opening {file_path}")
        with open(file_path, "rb") as definition_file:
            tb_definition = json.load(definition_file)
            description = tb_definition.get(
                "description",
                'No description')
            location_prefix = tb_definition.get(
                "location_prefix",
                "").strip(os.path.sep)
            stage_bucket = aws_s3.Bucket.from_bucket_arn(
                self, id=bucket,
                bucket_arn="arn:aws:s3:::{}".format(
                    bucket
                    )
                )
            logging.info("Defining columns and partitions")
            partition_keys = []
            columns = []
            for column in tb_definition.get("columns", []):
                glue_column = glue.CfnTable.ColumnProperty(
                    name=column["name"],
                    type=column["type"],
                    comment=column.get(
                        "comment",
                        'No comment'
                        )
                    )
                if column.get("partition", False):
                    partition_keys.append(glue_column)
                else:
                    columns.append(glue_column)

            logging.info(f"Creating '{self.db_name}' database")
            self.create_db(self.db_name, self.aws_account_id, bucket)

            logging.info(f"Creating '{self.tb_name}' Glue table")
            self.create_table(self.tb_name, self.db_name, partition_keys, columns, bucket, self.aws_account_id)

    def create_db(self, db_name, account_id, stage_bucket):
        cfn_database = glue.CfnDatabase(self,
        id=db_name,
        catalog_id=account_id,
        database_input=glue.CfnDatabase.DatabaseInputProperty(
            description="Database for store uber movements",
            location_uri=stage_bucket,
            name=db_name
        ))
        return cfn_database

    def create_table(self, tb_name, db_name, partition_keys, columns, s3_bucket, account_id):
        cfn_table = glue.CfnTable(
            self,
            id=tb_name,
            catalog_id=account_id,
            database_name=db_name,
            table_input=glue.CfnTable.TableInputProperty(
                partition_keys=partition_keys,
                name=tb_name,
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=columns,
                    location=s3_bucket,
                    output_format='PARQUET'
                    ))
        )
        return cfn_table
