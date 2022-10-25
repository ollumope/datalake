import logging
import json
import os
from aws_cdk import aws_glue as glue
import aws_cdk.aws_s3 as aws_s3
from constructs import Construct

class CreateGlueTable(Construct):
    # Create AWS Glue databases & tables according to their definitions
    def __init__(self, scope: Construct, id: str,
                 aws_account_id: str,
                 bucket: str, tb_name:str,
                 db_name:str, logging:logging.Logger, **kwargs):
        super().__init__(scope, id, **kwargs)
        self.aws_account_id = aws_account_id
        self.tb_name = tb_name
        base_path = os.getcwd()

        glue_catalog_path = f"{base_path}/lake_iac/glue/"
        file_path = os.path.normpath(
            f"{glue_catalog_path}/{self.tb_name}_definition.json"
            )
        logging.info(f"Opening {file_path} and saving columns definition")
        with open(file_path, "rb") as definition_file:
            tb_definition = json.load(definition_file)
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


            logging.info(f"Creating '{self.tb_name}' Glue table")
            self.create_table(self.tb_name, db_name, partition_keys, columns, bucket, self.aws_account_id)

    def create_table(self, tb_name, db_name, partition_keys, columns, s3_bucket_name, account_id):
        '''
        Create Glue table
        input:
            tb_name:
            db_name:
            partition_keys:
            columns:
            s3_bucket_name:
            account_id:
        output:
            cfn_table: Database definition
        '''
        cfn_table = glue.CfnTable(
            self,
            id=tb_name,
            catalog_id=account_id,
            database_name=db_name,
            table_input=glue.CfnTable.TableInputProperty(
                partition_keys=partition_keys,
                name=tb_name,
                table_type="EXTERNAL_TABLE",
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=columns,
                    location=f's3://{s3_bucket_name}',
                    input_format='org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    output_format='org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        name="SerDe",
                        parameters={'serialization.format':1},
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                        )
                    )
            )
        )
        return cfn_table
