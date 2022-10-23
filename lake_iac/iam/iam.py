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
                    "glue:*",
                    "s3:GetBucketLocation",
                    "s3:ListBucket",
                    "s3:ListAllMyBuckets",
                    "s3:GetBucketAcl",
                    "iam:ListRolePolicies",
                    "iam:GetRole",
                    "iam:GetRolePolicy",
                    "logs:CreateLogGroup",
                    "logs:PutLogEvents",
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "glue:StartJobRun",
                ],
                resources = ['*']
            ))
        self.role.add_to_policy(
            cdk.aws_iam.PolicyStatement(
                effect = cdk.aws_iam.Effect.ALLOW,
                actions = [
                    "s3:GetObject",
                    "s3:PutObject"
                ],
                resources = ["arn:aws:s3:::olmontoy-lake-stage*",]
            )
        )
        self.role_arn = self.role.role_arn
        f'arn:aws:iam::{aws_account_id}:role/service-role/{identifier}'
    
    def create_iam_role(self, id, role_desc, service_ppal):
        '''Create role for a services
        input:
            id: role name and identifer
            role_desc: role description
            service_ppal:  The IAM principal which can assume this role
        output:
            role definition'''
        role = cdk.aws_iam.Role(
            self,
            id=id,
            role_name=id,
            description=role_desc,
            assumed_by=cdk.aws_iam.ServicePrincipal(service_ppal),
        )
        cdk.Tags.of(scope=role).add(
                    "service", "security"
                    )
        return role
