from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_rds as rds,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_s3 as s3,
    aws_sns as sns,
    aws_events as events,
    aws_events_targets as targets,
    Duration
)
from constructs import Construct

class CdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Define Lambda layers
        pandas = lambda_.LayerVersion.from_layer_version_attributes(self, 'Pandas',
            layer_version_arn="arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39-Arm64:10")

        
        # Define the function's execution role
        lambda_role = iam.Role(self, "lambda_role",
                    assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                    )
        lambda_role.add_to_policy(iam.PolicyStatement(
                    resources=["*"],
                    actions=[
                            "s3:GetObject",
                            "s3:PutObject",
                            "sns:Publish",
                            "secretsmanager:GetSecretValue"]
                    ))
        # Create the bucket used to store the data
        s3_bucket = s3.Bucket(self, 'devops-serverless-etl-poc')

        
        # Create the function
        function = lambda_.Function(self, "Serverless-ETL",
                    runtime=lambda_.Runtime.PYTHON_3_9,
                    code=lambda_.Code.from_asset("./devops_serverless_etl"),
                    handler="lambda_function.lambda_handler",
                    layers=[pandas],
                    timeout=Duration.minutes(5),
                    role=lambda_role,
                    memory_size=128,
                    environment={
                        'S3_BUCKET':s3_bucket.bucket_name
                        
                    }
                    )

        