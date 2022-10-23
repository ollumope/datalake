import logging
import json
import os
import aws_cdk as cdk
import ast
from constructs import Construct


class LambdaFunction(Construct):
    def __init__(self,
        scope:Construct,
        identifier:str,
        job_name:str,
        role_arn:str,
        logger:logging.Logger,
        event_id:str,
        bucket_raw:str,
        bucket_stage:str,
        db_name:str,
        tb_name:str,
        **kwargs):

        super().__init__(scope, identifier, **kwargs)
        base_path = os.getcwd()
        self.log = logger
        self.role_arn = role_arn

        lambda_def_path = f"{base_path}/lake_iac/lambda_fn/lambda_definition.json"
        # lambda code local
        lambda_code_path = f'{base_path}/src/lambda/'

        # Loading and parsing "definition" for Lambda function file.
        self.log.info(f"Loading Lambda definition:{lambda_def_path}")
        with open(lambda_def_path, "rb") as file:
            lambda_data = json.load(file)
        
        # Lambda varialbes
        self.function_name = f"initialize-{job_name}"
        environment_variables = {
            'GLUE_JOB_NAME': job_name,
            'BUCKET_RAW':bucket_raw,
            'BUCKET_STAGE':bucket_stage,
            'DATABASE_NAME':db_name,
            'TABLE_NAME':tb_name
            }
        logger.info(f'Creating lambda for glue job execution')
        lambda_fn = self.create_lambda_function(self.function_name, lambda_code_path, lambda_data, environment_variables)
        self.create_event(event_id, bucket_raw, lambda_fn)

    def create_lambda_function(self, lambda_function_handler_name,
                               script_location, data,
                               environment_variables) -> cdk.aws_lambda.Function:
        property_python_version = data['property_python_version']
        memory_size = data['memory_size']
        timeout = data['timeout']
        self.log.info(f"Creating {lambda_function_handler_name} lambda")
        lambda_template = cdk.aws_lambda.Function(
            self, id=lambda_function_handler_name,
            runtime=cdk.aws_lambda.Runtime(str(property_python_version)),
            code=cdk.aws_lambda.Code.from_asset(script_location), 
            handler='main.lambda_handler',
            function_name=lambda_function_handler_name,
            memory_size=memory_size,
            role=cdk.aws_iam.Role.from_role_arn(
                self,
                id=f'{lambda_function_handler_name}_role',
                role_arn=self.role_arn,
            ),
            environment=environment_variables,
            timeout=cdk.Duration.seconds(timeout),
            description='Glob job initialization'
        )
        return lambda_template

    def create_event(self, identifier, bucket_raw, lambda_fn):
        self.log.info("Create envent pattern for s3")
        event_pattern = cdk.aws_events.EventPattern(
            source=["aws.s3"],
            detail_type=["Object Created"],
            detail={
                "bucket": [f"{bucket_raw}"]
                }
            )

        self.log.info("Create rule")
        event_rule = cdk.aws_events.Rule(
            self,
            id=identifier,
            description="Rule to detect when a new file came into s3 bucket",
            event_pattern=event_pattern,
            rule_name=identifier)

        self.log.info("Add event rule target")
        event_rule.add_target(cdk.aws_events_targets.LambdaFunction(
            lambda_fn))

