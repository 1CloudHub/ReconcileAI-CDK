from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_iam as iam,
    RemovalPolicy,
    aws_lambda as lambda_,
      aws_bedrock as bedrock,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    aws_s3_deployment as s3deploy,
    CustomResource,
    Duration,
    custom_resources as cr,
    CfnOutput,
    aws_opensearchserverless as opensearch,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
        aws_rds as rds,
        aws_apigateway as apigateway,
    aws_apigatewayv2 as apigatewayv2,
    aws_apigatewayv2_integrations as apigatewayv2_integrations,
    aws_lambda_event_sources as lambda_event_sources,
    RemovalPolicy,
    aws_s3_deployment as s3deploy,
    CfnOutput,
    Duration,
    CfnOutput,
    Size
    
)

import json 
import random
from pathlib import Path
import string
import time
from aws_cdk import Tags
import boto3
import os
from constructs import Construct

def generate_random_alphanumeric(length=6):
    """
    Generates a random name that follows AWS naming requirements.
    - Must be between 3 and 32 characters for most AWS resources.
    - Only contains lowercase letters, numbers, and hyphens.
    - Starts with a lowercase letter.
    - Ends with a lowercase letter or a number.
    """
    if not 3 <= length <= 32:
        raise ValueError("Length must be between 3 and 32 characters.")

    # Characters for the main body of the name (excluding hyphens at start/end)
    body_chars = string.ascii_lowercase + string.digits

    # Characters allowed at the end of the name
    end_chars = string.ascii_lowercase + string.digits

    # Generate the first character (must be lowercase letter)
    first_char = random.choice(string.ascii_lowercase)
    
    # Generate the middle characters (can include hyphens but not at start/end)
    if length > 2:
        middle_chars = ''.join(random.choices(body_chars + '-', k=length - 2))
        # Ensure no consecutive hyphens and no hyphen at the end
        middle_chars = middle_chars.replace('--', '-')
        if middle_chars.endswith('-'):
            middle_chars = middle_chars[:-1] + random.choice(string.ascii_lowercase + string.digits)
    else:
        middle_chars = ''

    # Generate a valid final character
    last_char = random.choice(end_chars)

    return first_char + middle_chars + last_char



def generate_lambda_safe_name(length=12):
    """
    Generates a random name that is safe for Lambda functions.
    - Only contains letters, numbers, hyphens, and underscores
    - No periods or other special characters
    """
    if not 3 <= length <= 63:
        raise ValueError("Length must be between 3 and 63 characters.")

    # Characters for Lambda-safe names (no periods)
    body_chars = string.ascii_lowercase + string.digits + '-_'

    # Characters allowed at the end of the name
    end_chars = string.ascii_lowercase + string.digits

    # Generate the first n-1 characters
    main_part = ''.join(random.choices(body_chars, k=length - 1))

    # Generate a valid final character
    last_char = random.choice(end_chars)

    return "q" + main_part + last_char


def generate_rds_safe_name(length=12):
    """
    Generates a random name that is safe for RDS database names.
    - Only contains letters and numbers (no hyphens, underscores, or special characters)
    - Must begin with a letter
    """
    if not 3 <= length <= 63:
        raise ValueError("Length must be between 3 and 63 characters.")

    # Characters for RDS-safe names (only letters and numbers)
    body_chars = string.ascii_lowercase + string.digits

    # Characters allowed at the end of the name
    end_chars = string.ascii_lowercase + string.digits

    # Generate the first n-1 characters
    main_part = ''.join(random.choices(body_chars, k=length - 1))

    # Generate a valid final character
    last_char = random.choice(end_chars)

    return "q" + main_part + last_char



unique_key = generate_random_alphanumeric(8)

lambda_safe_key = generate_lambda_safe_name()
rds_safe_key = generate_rds_safe_name()

class CdkCodeStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here
        
        vpc = ec2.Vpc(
            self, "VPC"+unique_key,
            ip_protocol=ec2.IpProtocol.IPV4_ONLY,
            max_azs=2,
            cidr="10.0.0.0/16",
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="PublicSubnet",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="PrivateSubnet",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                )
            ]
        )
        
        # Create security group for EC2
        ec2_security_group = ec2.SecurityGroup(
            self, "MyEC2SecurityGroup",
            vpc=vpc,
            description="Security group for EC2 instance",
            allow_all_outbound=True
        )

        ec2_security_group.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(22),
            description="Allow SSH access"
        )

        ec2_security_group.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(80),
            description="Allow HTTP access"
        )

        ec2_security_group.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(8000),
            description="Allow HTTP access"
        )

         # Create security group for RDS
        rds_security_group = ec2.SecurityGroup(
            self, "RDSSecurityGroup",
            vpc=vpc,
            description="Security group for RDS instance",
            allow_all_outbound=False
        )
        
        # Create Lambda security group
        lambda_security_group = ec2.SecurityGroup(
            self, "LambdaSecurityGroup",
            vpc=vpc,
            description="Security group for Lambda functions",
            allow_all_outbound=True
        )
        
        # Allow Lambda to access RDS
        rds_security_group.add_ingress_rule(
            peer=lambda_security_group,
            connection=ec2.Port.tcp(5432),
            description="Allow PostgreSQL access from Lambda"
        )

        # Also allow Lambda security group to access RDS (explicit rule)
        lambda_security_group.add_egress_rule(
            peer=rds_security_group,
            connection=ec2.Port.tcp(5432),
            description="Allow Lambda to connect to RDS"
        )
        
        key_pair = ec2.KeyPair(
            self, "MyKeyPair",
            key_pair_name=f"keypair-{unique_key}",  # Use your random name
            type=ec2.KeyPairType.RSA,
            format=ec2.KeyPairFormat.PEM
        )

        # Allow EC2 to access RDS
        rds_security_group.add_ingress_rule(
            peer=ec2_security_group,
            connection=ec2.Port.tcp(5432),
            description="Allow PostgreSQL access from EC2"
        )

        # Create RDS subnet group
        db_subnet_group = rds.SubnetGroup(
            self, "MyDBSubnetGroup",
            description="Subnet group for RDS database",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            )
        )  
        s3_bucket_name = "sap-data-bucket-" + unique_key
        frontend_bucket_name = "frontend-bucket-" + unique_key
        
        
        data_bucket = s3.Bucket(
            self,
            "databucket",
            bucket_name=s3_bucket_name,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,  # For development only
            auto_delete_objects=True,  # For development only
        )
            
        
        # Frontend bucket for static website hosting (public)
        frontend_bucket = s3.Bucket(
            self, 
            "frontendbucket",
            bucket_name=frontend_bucket_name,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,  # For development only
            auto_delete_objects=True,  # For development only
            website_index_document="index.html",
            website_error_document="index.html",
            # public_read_access=True,  # Allow public read access
            # block_public_access=s3.BlockPublicAccess.BLOCK_NONE  # Disable public access blocking
        )
        
        s3deploy.BucketDeployment(
            self,
            "mocksapdatadeployment",
            destination_bucket=data_bucket,
            destination_key_prefix="data/",  # Folder inside S3 bucket
            sources=[
                s3deploy.Source.asset("./mock_sap_data")  # Local folder
            ],
            retain_on_delete=False
        )
        
        s3deploy.BucketDeployment(
            self,
            "sopdocumentsdeployment",
            destination_bucket=data_bucket,
            destination_key_prefix="sopdocument/",  # Folder inside S3 bucket
            sources=[
                s3deploy.Source.asset("./sample_sop_documents")  # Local folder
            ],
            retain_on_delete=False
        )
        
        # Upload frontend folder contents to the frontend bucket
        s3deploy.BucketDeployment(
            self,
            "deployfrontendfolder",
            sources=[s3deploy.Source.asset("frontend")],  # Path to your frontend folder
            destination_bucket=frontend_bucket,
            destination_key_prefix="",  # Upload to root of bucket
        )
        
        # ---------------------------------------------------------
        # Create IAM Role for ReconcileAI Lambda
        # ---------------------------------------------------------
        lambda_role = iam.Role(
            self,
            "ReconcileAILambdaRole",
            role_name="ReconcileAI_Lambda_Role_" + unique_key,
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )
        
        
        managed_policies = [
            "AmazonAPIGatewayAdministrator",
            "AmazonAPIGatewayInvokeFullAccess",
            "AmazonBedrockFullAccess",
            "AmazonCognitoPowerUser",
            "AmazonEC2FullAccess",
            "AmazonRDSFullAccess",
            "AmazonS3FullAccess",
            "AmazonSESFullAccess",
            "service-role/AWSLambdaVPCAccessExecutionRole"
        ]
        
        for policy_name in managed_policies:
            lambda_role.add_managed_policy(
                iam.ManagedPolicy.from_aws_managed_policy_name(policy_name)
            )
        
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "secretsmanager:GetSecretValue"
                ],
                resources=[
                    f"arn:aws:secretsmanager:us-west-2:{self.account}:secret:rds-credentials-{unique_key}*"
                ]
            )
        )
        
        
        boto3_layer = lambda_.LayerVersion(
            self,
            "Boto3Layer",
            layer_version_name="boto3-layer-" + unique_key,
            code=lambda_.Code.from_asset("lambda_layers/boto3.zip"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="Boto3 and botocore upgraded layer"
        )
        
        mcp_v2_layer = lambda_.LayerVersion(
            self,
            "McpV2Layer",
            layer_version_name="mcp-v2-layer-" + unique_key,
            code=lambda_.Code.from_asset("lambda_layers/mcp_v2.zip"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="MCP v2 SDK dependency layer"
        )
        
        reconcileai_lambda_function = lambda_.Function(
            self,
            "ReconcileAILambdaFunction",
            function_name="reconcileai_lambda_" + lambda_safe_key,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="ReconcileAI_lambda.lambda_handler",
            code=lambda_.Code.from_asset("lambda"),
            timeout=Duration.seconds(600),
            vpc=vpc,
            security_groups=[lambda_security_group],
            role=lambda_role,
            environment={
                "DATA_BUCKET_NAME": data_bucket.bucket_name,
                "FRONTEND_BUCKET_NAME": frontend_bucket.bucket_name
            }
            
        )
        
        reconcileai_lambda_function.add_layers(boto3_layer)
        reconcileai_lambda_function.add_layers(mcp_v2_layer)
        
        websocket_lambda = lambda_.Function(
            self,
            "WebSocketHandlerLambda",
            function_name="websocket_handler_" + lambda_safe_key,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="websocket_handler.lambda_handler",  # file: websocket_handler.py
            code=lambda_.Code.from_asset("lambda"),
            timeout=Duration.seconds(30),
            role=lambda_role,  # reuse your existing Lambda role
            environment={
                "WEBSOCKET_REGION": self.region,
                # Endpoint will be filled after API is created
                "WEBSOCKET_ENDPOINT": ""  
            }
        )
        
        
        #REST API
        sap_api = apigateway.RestApi(
            self, "sapapigateway",
            rest_api_name="sap_rest_api",
            description="API Gateway for SAP data access",
            binary_media_types=["multipart/form-data"],
            deploy_options=apigateway.StageOptions(
                stage_name="dev",
                logging_level=apigateway.MethodLoggingLevel.OFF,
                data_trace_enabled=False
                )
            )
        #/erp resource starts here
        erp_resource = sap_api.root.add_resource("ERP")

        # OPTIONS (CORS Preflight)
        erp_resource.add_method(
            "OPTIONS",
            apigateway.MockIntegration(
                integration_responses=[
                    apigateway.IntegrationResponse(
                        status_code="200",
                        response_parameters={
                            "method.response.header.Access-Control-Allow-Headers": "'*'",
                            "method.response.header.Access-Control-Allow-Methods": "'OPTIONS,POST'",
                            "method.response.header.Access-Control-Allow-Origin": "'*'"
                        },
                        response_templates={
                            "application/json": ""
                        }
                    )
                ],
                passthrough_behavior=apigateway.PassthroughBehavior.WHEN_NO_MATCH,
                request_templates={"application/json": "{\"statusCode\": 200}"}
            ),
            method_responses=[
                apigateway.MethodResponse(
                    status_code="200",
                    response_parameters={
                        "method.response.header.Access-Control-Allow-Headers": True,
                        "method.response.header.Access-Control-Allow-Methods": True,
                        "method.response.header.Access-Control-Allow-Origin": True
                    }
                )
            ]
        )

        # POST (Lambda Integration)
        erp_resource.add_method(
            "POST",
            apigateway.LambdaIntegration(reconcileai_lambda_function),
            authorization_type=apigateway.AuthorizationType.NONE,
            method_responses=[
                apigateway.MethodResponse(status_code="200")
            ]
        )
        
        
        #erp resource ends here


        websocket_api = apigatewayv2.WebSocketApi(
            self,
            "SAPWebSocketAPI" + unique_key,
            api_name="SAP_ws_" + unique_key,
        )
        
        # $connect
        websocket_api.add_route(
            "$connect",
            integration=apigatewayv2_integrations.WebSocketLambdaIntegration(
                "WSConnectIntegration",
                websocket_lambda
            )
        )

        # $disconnect
        websocket_api.add_route(
            "$disconnect",
            integration=apigatewayv2_integrations.WebSocketLambdaIntegration(
                "WSDisconnectIntegration",
                websocket_lambda
            )
        )

        # $default
        websocket_api.add_route(
            "$default",
            integration=apigatewayv2_integrations.WebSocketLambdaIntegration(
                "WSDefaultIntegration",
                websocket_lambda
            )
        )

        # Custom route: sendMessage
        websocket_api.add_route(
            "sendMessage",
            integration=apigatewayv2_integrations.WebSocketLambdaIntegration(
                "WSSendMessageIntegration",
                websocket_lambda
            )
        )
        
        
        websocket_stage = apigatewayv2.WebSocketStage(
            self,
            "SAPWebSocketStage",
            web_socket_api=websocket_api,
            stage_name="dev",
            auto_deploy=True
        )
        
        websocket_lambda.add_permission(
            "InvokeByWebSocketAPI",
            principal=iam.ServicePrincipal("apigateway.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"arn:aws:execute-api:{self.region}:{self.account}:{websocket_api.api_id}/*"
        )
        
        
        websocket_url = f"wss://{websocket_api.api_id}.execute-api.{self.region}.amazonaws.com/dev"

        websocket_lambda.add_environment("WEBSOCKET_ENDPOINT", websocket_url)
        websocket_lambda.add_environment("WEBSOCKET_REGION", self.region)
        
        # Create RDS PostgreSQL instance
        db_instance = rds.DatabaseInstance(
            self, "MyPostgreSQLDB",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_17_4
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3,
                ec2.InstanceSize.MICRO
            ),
            vpc=vpc,
            subnet_group=db_subnet_group,
            security_groups=[rds_security_group],  # Use the correct security group
            credentials=rds.Credentials.from_generated_secret(
                username="postgres",
                secret_name=f"rds-credentials-{unique_key}"  # Make it unique
            ),
            allocated_storage=20,
            storage_type=rds.StorageType.GP2,
            deletion_protection=False,
            delete_automated_backups=False,
            backup_retention=Duration.days(7),
            removal_policy=RemovalPolicy.DESTROY,
            database_name=rds_safe_key
        )
        
        
        reconcileai_lambda_function.add_environment("RDS_ENDPOINT", db_instance.db_instance_endpoint_address)
        reconcileai_lambda_function.add_environment("db_host", db_instance.db_instance_endpoint_address)
        reconcileai_lambda_function.add_environment("db_name", "postgres")
        reconcileai_lambda_function.add_environment("db_port", "5432")
        reconcileai_lambda_function.add_environment("db_password", f"rds-credentials-{unique_key}")
        reconcileai_lambda_function.add_environment("region_name", self.region)
        reconcileai_lambda_function.add_environment("region_used", self.region)
        
        CfnOutput(
            self,
            "WebSocketURL",
            value=websocket_url,
            description="WebSocket connection URL"
        )
        
        
        


