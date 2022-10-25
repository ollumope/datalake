import logging
import json
import aws_cdk as cdk
import ast
from constructs import Construct


class CreatePermission(Construct):
    def __init__(self,
        scope:Construct,
        identifier:str,
        role_desc:str,
        service_ppal:str,
        aws_account_id:str,
        logging:logging.Logger,
        **kwargs):

        super().__init__(scope, identifier, **kwargs)
        self.aws_account_id = aws_account_id
        logging.info('Creating role with policies')
        self.role = self.create_iam_role(identifier,role_desc, service_ppal)
        logging.info('Assing policies')
        self.role.add_to_policy(
            cdk.aws_iam.PolicyStatement(
                effect = cdk.aws_iam.Effect.ALLOW,
                actions = [
                    "s3:*",
                    "s3-object-lambda:*",
                    "glue:StartJobRun",
                    "logs:CreateLogGroup",
                    "logs:PutLogEvents",
                    "glue:*"
                ],
                resources = [
                    "*",
                    "arn:aws:s3:::olmontoy-lake-stage*",
                    f"arn:aws:logs:us-east-1:{aws_account_id}:*",
                    f"arn:aws:logs:us-east-1:{aws_account_id}:log-group:/aws/lambda/*"
                    ]
            )
        )
        self.role_arn = self.role.role_arn
        f'arn:aws:iam::{aws_account_id}:role/{identifier}'
    
    def create_iam_role(self, id, role_desc, service_ppal):
        '''Create role for a services
        input:
            id: role name and identifer
            role_desc: role description
            service_ppal:  The IAM principal which can assume this role
        output:
            role object definition'''
        role = cdk.aws_iam.Role(
            self,
            id=id,
            role_name=id,
            description=role_desc,
            assumed_by=cdk.aws_iam.ServicePrincipal(service_ppal),
            managed_policies=[
                cdk.aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                'service-role/AWSGlueServiceRole'),
            ]

        )
        return role
