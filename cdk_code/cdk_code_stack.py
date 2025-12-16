from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_cognito as cognito,
    aws_s3 as s3,
    aws_iam as iam,
    RemovalPolicy,
    aws_lambda as lambda_,
      aws_bedrock as bedrock,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    aws_s3_deployment as s3deploy,
    aws_cognito as cognito,
    aws_secretsmanager as secretsmanager,
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

def generate_aws_compliant_password(length: int = 16) -> str:
    """
    Generates a random AWS Cognito–compliant password.

    Rules satisfied:
    - Minimum length >= 12 (recommended)
    - At least one uppercase letter
    - At least one lowercase letter
    - At least one digit
    - At least one special character
    - No spaces

    :param length: Total password length (must be >= 12)
    :return: Secure random password string
    """

    if length < 12:
        raise ValueError("Password length must be at least 12 characters")

    lowercase = string.ascii_lowercase
    uppercase = string.ascii_uppercase
    digits = string.digits

    # Cognito-safe special characters
    special = "!@#$%^&*()-_=+[]{}<>?"

    # Ensure rule compliance
    password_chars = [
        random.choice(lowercase),
        random.choice(uppercase),
        random.choice(digits),
        random.choice(special),
    ]

    # Fill remaining length
    all_chars = lowercase + uppercase + digits + special
    remaining_length = length - len(password_chars)

    password_chars.extend(
        random.choice(all_chars) for _ in range(remaining_length)
    )

    # Shuffle to avoid predictable order
    random.shuffle(password_chars)

    return "".join(password_chars)


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
        ec2_security_group.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(8084),
            description="Allow HTTP access"
        )
        ec2_security_group.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(8085),
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
        
        
        #lambda environment variables for RDS connection
        reconcileai_lambda_function.add_environment("RDS_ENDPOINT", db_instance.db_instance_endpoint_address)
        reconcileai_lambda_function.add_environment("db_host", db_instance.db_instance_endpoint_address)
        reconcileai_lambda_function.add_environment("db_name", "postgres")
        reconcileai_lambda_function.add_environment("db_port", "5432")
        reconcileai_lambda_function.add_environment("db_password", f"rds-credentials-{unique_key}")
        reconcileai_lambda_function.add_environment("region_name", self.region)
        reconcileai_lambda_function.add_environment("region_used", self.region)


        USER_EMAIL = "user@reconcileai.com"
        USERNAME = USER_EMAIL  # Cognito requires a username internally
        PASSWORD = generate_aws_compliant_password()
        
        user_pool = cognito.UserPool(
            self,
            "UserPool",
            self_sign_up_enabled=False,
            sign_in_aliases=cognito.SignInAliases(
                email=True,
                username=False
            ),
            password_policy=cognito.PasswordPolicy(
                min_length=12,
                require_lowercase=True,
                require_uppercase=True,
                require_digits=True,
                require_symbols=True
            ),
            account_recovery=cognito.AccountRecovery.EMAIL_ONLY
        )

        # --------------------------------------------------
        # Create User via AdminCreateUser
        # --------------------------------------------------
        create_user = cr.AwsCustomResource(
            self,
            "CreateCognitoUser",
            on_create=cr.AwsSdkCall(
                service="CognitoIdentityServiceProvider",
                action="adminCreateUser",
                parameters={
                    "UserPoolId": user_pool.user_pool_id,
                    "Username": USERNAME,
                    "UserAttributes": [
                        {"Name": "email", "Value": USER_EMAIL},
                        {"Name": "email_verified", "Value": "true"}
                    ],
                    "MessageAction": "SUPPRESS"
                },
                physical_resource_id=cr.PhysicalResourceId.of(
                    f"{USERNAME}-user"
                )
            ),
            policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                resources=[user_pool.user_pool_arn]
            )
        )

        # --------------------------------------------------
        # Set PERMANENT password
        # --------------------------------------------------
        set_password = cr.AwsCustomResource(
            self,
            "SetPermanentPassword",
            on_create=cr.AwsSdkCall(
                service="CognitoIdentityServiceProvider",
                action="adminSetUserPassword",
                parameters={
                    "UserPoolId": user_pool.user_pool_id,
                    "Username": USERNAME,
                    "Password": PASSWORD,
                    "Permanent": True
                },
                physical_resource_id=cr.PhysicalResourceId.of(
                    f"{USERNAME}-password"
                )
            ),
            policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                resources=[user_pool.user_pool_arn]
            ),
            timeout=Duration.minutes(2)
        )

        set_password.node.add_dependency(create_user)
        
        
        
        CfnOutput(self, "UserPoolId", value=user_pool.user_pool_id)
        CfnOutput(self, "LoginEmail", value=USER_EMAIL)
        CfnOutput(self, "LoginPassword", value=PASSWORD)
        
        CfnOutput(
            self,
            "WebSocketURL",
            value=websocket_url,
            description="WebSocket connection URL"
        )
        
        
        ec2_role = iam.Role(
            self, "EC2Role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3ReadOnlyAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess")
            ],
                inline_policies={
        "TranscribePolicy": iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "transcribe:StartTranscriptionJob",
                        "transcribe:GetTranscriptionJob", 
                        "transcribe:DeleteTranscriptionJob"
                    ],
                    resources=["*"]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject"
                    ],
                    resources=[f"arn:aws:s3:::{s3_bucket_name}/*"]
                )
            ]
        )
    }
        )
        instance_profile = iam.CfnInstanceProfile(
    self, "EC2InstanceProfile",
    roles=[ec2_role.role_name]
)

        ec2_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "ec2-instance-connect:SendSSHPublicKey",
                "ec2:DescribeInstances",
                "ec2:DescribeInstanceAttribute"
            ],
            resources=["*"]
        ))

        ec2_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret"
            ],
            resources=[
                f"arn:aws:secretsmanager:*:*:secret:rds-credentials-{unique_key}-*"
            ]
        ))

        # AdministratorAccess provides wide permissions needed for provisioning and bootstrap tasks
        # IMPORTANT: Grant EC2 access to the RDS secret
        if db_instance.secret:
            db_instance.secret.grant_read(ec2_role)    



        ec2_instance = ec2.Instance(
            self, "MyEC2Instance",
            role=ec2_role,
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3,
                ec2.InstanceSize.MEDIUM
            ),
            # machine_image=ec2.MachineImage.latest_amazon_linux2(),
            machine_image=ec2.MachineImage.lookup(
                name="Deep Learning OSS Nvidia Driver AMI GPU PyTorch 2.7 (Ubuntu 22.04)*",
                owners=["amazon"]
            ),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC
            ),
            security_group=ec2_security_group,
            key_pair=key_pair,
            user_data=ec2.UserData.for_linux(),
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/sda1",  # Root volume device name for Ubuntu
                    volume=ec2.BlockDeviceVolume.ebs(
                        volume_size=300,  # Size in GB
                        volume_type=ec2.EbsDeviceVolumeType.GP3,  # GP3 is cost-effective and performant
                        delete_on_termination=True,  # Delete when instance terminates
                        encrypted=True  # Optional: encrypt the volume
                    )
                )
            ]
        )
        secret_name = f"rds-credentials-{unique_key}"
        ec2_instance.add_user_data(
     "sudo apt update -y",
    "sudo apt install -y apache2 awscli jq postgresql-client-14",
    "sudo apt install -y nodejs npm", 
    "systemctl start apache2",
    "systemctl enable apache2", 
    "echo '<h1>Hello from AWSSSSSSSSSSSSSS!</h1>' > /var/www/html/index.html",
    'cd home/ubuntu/',
    'mkdir startingggggg',
    'mkdir final'
    # Create restoration script (note: using /home/ubuntu for Ubuntu AMI)
    'cat << \'EOF\' > /home/ubuntu/restore_db.sh',
    '#!/bin/bash',
    'set -e',
    ''

    'EOF',    
    'mkdir creating_voicebittttttttt',
    'cat << \'EOF\' > /home/ubuntu/voice_bot.sh',
    '#!/bin/bash',
    'set -e',
    '',
    'export DEBIAN_FRONTEND=noninteractive',
    'echo "Getting database credentials from Secrets Manager..."',    
    'sudo apt-get update -y',
    'sudo apt-get install -y postgresql postgresql-contrib',
    '# Start and enable PostgreSQL',
    'sudo systemctl enable postgresql',
    'sudo systemctl start postgresql',
    "sudo systemctl restart postgresql || echo 'PostgreSQL restart failed'",
    "git clone --branch ec2_sap https://github.com/1CloudHub/ReconcileAI-CDK.git",
    "cd ReconcileAI-CDK",
    "pip install -r requirements.txt",
    "sudo npm install -g aws-cdk",
    f'SECRET_JSON=$(aws secretsmanager get-secret-value --secret-id "{secret_name}" --query SecretString --output text --region {self.region})',
    'echo "$SECRET_JSON"',
    'DB_HOST=$(echo "$SECRET_JSON" | jq -r .host)',
    'DB_PORT=$(echo "$SECRET_JSON" | jq -r .port)',
    'DB_USERNAME=$(echo "$SECRET_JSON" | jq -r .username)',
    'DB_PASSWORD=$(echo "$SECRET_JSON" | jq -r .password)',
    'DB_NAME=$(echo "$SECRET_JSON" | jq -r .dbname)',
    "export DB_HOST=$(echo \"$SECRET_JSON\" | jq -r .host)",
    "export DB_PORT=$(echo \"$SECRET_JSON\" | jq -r .port)",
    "export DB_USERNAME=$(echo \"$SECRET_JSON\" | jq -r .username)",
    "export DB_PASSWORD=$(echo \"$SECRET_JSON\" | jq -r .password)",
    "export DB_NAME=$(echo \"$SECRET_JSON\" | jq -r .dbname)",
    f"export REGION={self.region}",
    # f"export STACK_SELECTION={self.stack_selection}",
    "",
    "echo 'Database connection details:'",
    "echo \"Host: $DB_HOST\"",
    "echo \"Port: $DB_PORT\"",
    "echo \"Database: $DB_NAME\"",
    "echo \"Username: $DB_USERNAME\"",
    "",
    '',
    'echo "Database connection details:"',
    'echo "Host: $DB_HOST"',
    'echo "Port: $DB_PORT"',
    'echo "Database: $DB_NAME"',
    'echo "Username: $DB_USERNAME"',
    '',
    'export PGPASSWORD="$DB_PASSWORD"',
    '',
    '# Test connection',
    'echo "Testing database connection..."',
    'psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USERNAME" -d "$DB_NAME" -c "SELECT version();"',
    # '',
    # '# Download dump',
    'echo "Downloading database dump file..."',
    # 'aws s3 cp s3://sql-dumps-bucket/dump-postgres.sql /tmp/dump.sql',
    "git clone https://github.com/1CloudHub/aivolvex-genai-foundry.git",
    
    '',
    '# Restore database',
    'echo "Restoring database from dump file..."',
    'psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USERNAME" -d "$DB_NAME" -f ~/ReconcileAI-CDK/dump-postgres.sql',
    '',
    '# Verify restoration',
    'echo "Verifying restoration..."',
    'psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USERNAME" -d "$DB_NAME" -c "\\\\dn"',
    'psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USERNAME" -d "$DB_NAME" -c "\\\\dt erp.*"',
    '',
    'echo "Database restoration completed successfully!"',
    "echo 'starting python code implementation'",
    "export DEBIAN_FRONTEND=noninteractive",
    "cd /home/ubuntu",
    # "aws s3 sync s3://sql-dumps-bucket/ec2_needs/ ./ec2_needs/",
    "cd sap_erp",
    "sudo apt install python3.10-venv -y",
    "python3 -m venv vulture",
    "source vulture/bin/activate",
    "pip install -r requirements.txt --no-input",
    "pip install asgiref --no-input",   
    "# Set environment variable and run in screen session",
    "screen -dmS run_app bash -c 'source vulture/bin/activate && python new_main.py'",
    "cd ..",
    "cd agent_code",
    "python3 -m venv eagle",
    "source eagle/bin/activate",
    "pip install -r requirements.txt",
    "export SAP_HOST=https://sap.apj.int.appflow.sap.aws.dev/sap/bc/gui/sap/its/webgui#",
    "export SAP_USER=PARTNER1",
    "export SAP_PASSWORD=1CloudHub",
    "screen -dmS run_app bash -c 'source eagle/bin/activate && python agent.py'",
    #    "screen -dmS run_app bash -c 'source eagle/bin/activate && export S3_PATH=" + s3_name + " && uvicorn sun:asgi_app --host 0.0.0.0 --port 8000'",

    "echo 'DONE!!!!!!!!!!!!!!'",
    'EOF',
    'mkdir adding_permissionssssssss',
    'sudo chmod +x /home/ubuntu/restore_db.sh',
    'sudo chown ubuntu:ubuntu /home/ubuntu/restore_db.sh',

    'sudo chmod +x /home/ubuntu/voice_bot.sh', 
    'sudo chown ubuntu:ubuntu /home/ubuntu/voice_bot.sh',
    'mkdir permissions_addeddddddd',
    # # Wait for RDS to be ready and run restoration
    'sleep 20',
    #'sudo su - ubuntu -c "/home/ubuntu/restore_db.sh" > /var/log/db_restore.log 2>&1',
    "sleep 30",
    'sudo su - ubuntu -c "/home/ubuntu/voice_bot.sh" > /var/log/voice_bot.log 2>&1'
    )


        ec2_instance_front = ec2.Instance(
            self, "MyEC2InstanceFront",
            role=ec2_role,
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3,
                ec2.InstanceSize.MEDIUM
            ),
            machine_image=ec2.MachineImage.latest_amazon_linux2023(),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC
            ),
            security_group=ec2_security_group,
            key_pair=key_pair,
            user_data=ec2.UserData.for_linux(),
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/sda1",  # Root volume device name for Ubuntu
                    volume=ec2.BlockDeviceVolume.ebs(
                        volume_size=300,  # Size in GB
                        volume_type=ec2.EbsDeviceVolumeType.GP3,  # GP3 is cost-effective and performant
                        delete_on_termination=True,  # Delete when instance terminates
                        encrypted=True  # Optional: encrypt the volume
                    )
                )
            ]
        )
        # Set the environment variables that will be passed to the EC2 instance
        rest_api_name = f"genaifoundry-api{name_key}"
        websocket_api_name = f"GenAIFoundry_ws{name_key}"
        transcribe_api_name = f"coaching_assist_voiceops{name_key}"
        bucket_name = frontend_bucket_name
        region = self.region

        # Alternative approach: Use hardcoded API IDs or skip API Gateway lookup
        ec2_instance_front.add_user_data(
      "#!/bin/bash",
            "",
            "set -e  # Exit on any error",
            "",
            "echo \"🚀 Starting React deployment from S3...\"",
            "",
            "# Set environment variables from CDK",
            f"export REST_API_NAME=\"{rest_api_name}\"",
            f"export WEBSOCKET_API_NAME=\"{websocket_api_name}\"",
            f"export TRANSCRIBE_API_NAME=\"{transcribe_api_name}\"",
            f"export BUCKET_NAME=\"{bucket_name}\"",
            f"export REGION=\"{region}\"",
            f"export STACK_SELECTION=\"{self.stack_selection}\"",
            "",
            "# Helper function to check if a command exists",
            "command_exists() {",
            "    command -v \"$1\" &> /dev/null",
            "}",
            "",
            "# Check for required environment variables",
            "check_required_env_vars() {",
            "    local missing_vars=()",
            "   ",
            "    if [[ -z \"${REST_API_NAME:-}\" ]]; then",
            "        missing_vars+=(\"REST_API_NAME\")",
            "    fi",
            "   ",
            "    if [[ -z \"${WEBSOCKET_API_NAME:-}\" ]]; then",
            "        missing_vars+=(\"WEBSOCKET_API_NAME\")",
            "    fi",
            "   ",
            "    if [[ -z \"${TRANSCRIBE_API_NAME:-}\" ]]; then",
            "        missing_vars+=(\"TRANSCRIBE_API_NAME\")",
            "    fi",
            "   ",
            "    if [[ -z \"${BUCKET_NAME:-}\" ]]; then",
            "        missing_vars+=(\"BUCKET_NAME\")",
            "    fi",
            "   ",
            "    if [[ -z \"${REGION:-}\" ]]; then",
            "        missing_vars+=(\"REGION\")",
            "    fi",
            "   ",
            "    if [[ ${#missing_vars[@]} -gt 0 ]]; then",
            "        echo \"❌ Error: The following required environment variables are not set:\"",
            "        printf '   - %s\\n' \"${missing_vars[@]}\"",
            "        echo \"\"",
            "        echo \"Please export these variables before running the script:\"",
            "        echo \"  export REST_API_NAME=\\\"your-rest-api-name\\\"\"",
            "        echo \"  export WEBSOCKET_API_NAME=\\\"your-websocket-api-name\\\"\"",
            "        echo \"  export TRANSCRIBE_API_NAME=\\\"your-transcribe-api-name\\\"\"",
            "        echo \"  export BUCKET_NAME=\\\"your-s3-bucket-name\\\"\"",
            "        echo \"  export REGION=\\\"your-aws-region\\\"\"",
            "        echo \"  export STACK_SELECTION=\\\"your-stack-selection\\\"\"",
            "        exit 1",
            "    fi",
            "}",
            "",
            "# Check required environment variables",
            "echo \"🔍 Checking required environment variables...\"",
            "check_required_env_vars",
            "",
            "echo \"✅ All required environment variables are set:\"",
            "echo \"  REST_API_NAME:        ${REST_API_NAME}\"",
            "echo \"  WEBSOCKET_API_NAME:   ${WEBSOCKET_API_NAME}\"",
            "echo \"  TRANSCRIBE_API_NAME:  ${TRANSCRIBE_API_NAME}\"",
            "echo \"  BUCKET_NAME:          ${BUCKET_NAME}\"",
            "echo \"  REGION:               ${REGION}\"",
            "",
            "echo \"🔧 Checking and installing prerequisites...\"",
            "",
            "# Install unzip if not present",
            "if ! command_exists unzip; then",
            "    echo \"📦 Installing unzip...\"",
            "    sudo yum install -y unzip --allowerasing",
            "else",
            "    echo \"✅ unzip already installed\"",
            "fi",
            "",
            "# Install curl if not present",
            "if ! command_exists curl; then",
            "    echo \"📦 Installing curl...\"",
            "    sudo yum install -y curl --allowerasing",
            "else",
            "    echo \"✅ curl already installed\"",
            "fi",
            "",
            "# Install Node.js and npm if not present",
            "if ! command_exists node || ! command_exists npm; then",
            "    echo \"📦 Installing Node.js and npm...\"",
            "    curl -fsSL https://rpm.nodesource.com/setup_18.x | sudo bash -",
            "    sudo yum install -y nodejs --allowerasing",
            "else",
            "    echo \"✅ Node.js and npm already installed\"",
            "fi",
            "",
            "# Install AWS CLI v2 if not present",
            "if ! command_exists aws; then",
            "    echo \"📦 Installing AWS CLI v2...\"",
            "    curl \"https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip\" -o \"awscliv2.zip\"",
            "    unzip awscliv2.zip",
            "    sudo ./aws/install",
            "    rm -rf aws awscliv2.zip",
            "else",
            "    echo \"✅ AWS CLI already installed\"",
            "fi",
            "",
            "# Set variables",
            "WORK_DIR=~/react-app",
            "ZIP_FILE=\"src.zip\"",
            "S3_SOURCE_PATH=\"s3://${BUCKET_NAME}/${ZIP_FILE}\"",
            "",
            "echo \"📁 Creating work directory: $WORK_DIR\"",
            "mkdir -p \"$WORK_DIR\"",
            "cd \"$WORK_DIR\"",
            "",
            "echo \"📥 Downloading $ZIP_FILE from S3...\"",
            "aws s3 cp \"$S3_SOURCE_PATH\" . --region \"$REGION\"",
            "",
            "echo \"📂 Unzipping $ZIP_FILE...\"",
            "unzip -o \"$ZIP_FILE\"",
            "rm \"$ZIP_FILE\"",
            "",
            "# 📦 Install dependencies",
            "echo \"📦 Running npm install...\"",
            "npm install",
            "",
            "# 🌐 Extract API Gateway IDs",
            "echo \"🔍 Fetching API Gateway IDs...\"",
            "",
            "# REST APIs (API Gateway v1)",
            "get_rest_api_id_by_name() {",
            "    aws apigateway get-rest-apis \\",
            "      --region \"$REGION\" \\",
            "      --query \"items[?name=='$1'].id\" \\",
            "      --output text",
            "}",
            "",
            "# WebSocket APIs (API Gateway v2)",
            "get_ws_api_id_by_name() {",
            "    aws apigatewayv2 get-apis \\",
            "      --region \"$REGION\" \\",
            "      --query \"Items[?Name=='$1'].ApiId\" \\",
            "      --output text",
            "}",
            "",
            "API_ID_REST=$(get_rest_api_id_by_name \"$REST_API_NAME\")",
            "API_ID_WS=$(get_ws_api_id_by_name \"$WEBSOCKET_API_NAME\")",
            "API_ID_TRANSCRIBE=$(get_rest_api_id_by_name \"$TRANSCRIBE_API_NAME\")",
            "",
            "# Validate that API IDs were found",
            "if [[ -z \"$API_ID_REST\" ]]; then",
            "    echo \"❌ Error: Could not find REST API with name '$REST_API_NAME'\"",
            "    exit 1",
            "fi",
            "",
            "if [[ -z \"$API_ID_WS\" ]]; then",
            "    echo \"❌ Error: Could not find WebSocket API with name '$WEBSOCKET_API_NAME'\"",
            "    exit 1",
            "fi",
            "",
            "if [[ -z \"$API_ID_TRANSCRIBE\" ]]; then",
            "    echo \"❌ Error: Could not find Transcribe API with name '$TRANSCRIBE_API_NAME'\"",
            "    exit 1",
            "fi",
            "",
            "# Debug logging",
            "echo \"✅ Retrieved API IDs:\"",
            "echo \"  REST API (chat):      $API_ID_REST (from $REST_API_NAME)\"",
            "echo \"  WebSocket API:        $API_ID_WS (from $WEBSOCKET_API_NAME)\"",
            "echo \"  Transcribe API:       $API_ID_TRANSCRIBE (from $TRANSCRIBE_API_NAME)\"",
            "",
            "# Construct URLs",
            "VITE_API_BASE_URL=\"https://${API_ID_REST}.execute-api.${REGION}.amazonaws.com/dev/chat_api\"",
            "VITE_WEBSOCKET_URL=\"wss://${API_ID_WS}.execute-api.${REGION}.amazonaws.com/production/\"",
            "VITE_WEBSOCKET_URL_VOICEOPS=\"https://${API_ID_WS}.execute-api.${REGION}.amazonaws.com/production/\"",
            "VITE_TRANSCRIBE_API_URL=\"https://${API_ID_TRANSCRIBE}.execute-api.${REGION}.amazonaws.com/dev/transcribe\"",
            "",
            "# 📄 Update .env file",
            "ENV_FILE=\".env\"",
            "echo \"🛠 Updating environment variables in $ENV_FILE...\"",
            "",
            "update_env_var() {",
            "    local key=\"$1\"",
            "    local value=\"$2\"",
            "    if grep -q \"^$key=\" \"$ENV_FILE\"; then",
            "        sed -i \"s|^$key=.*|$key=$value|\" \"$ENV_FILE\"",
            "    else",
            "        echo \"$key=$value\" >> \"$ENV_FILE\"",
            "    fi",
            "}",
            "",
            "update_env_var \"VITE_API_BASE_URL\" \"$VITE_API_BASE_URL\"",
            "update_env_var \"VITE_WEBSOCKET_URL\" \"$VITE_WEBSOCKET_URL\"",
            "update_env_var \"VITE_WEBSOCKET_URL_VOICEOPS\" \"$VITE_WEBSOCKET_URL_VOICEOPS\"",
            "update_env_var \"VITE_TRANSCRIBE_API_URL\" \"$VITE_TRANSCRIBE_API_URL\"",
            "update_env_var \"VITE_STACK_SELECTION\" \"$STACK_SELECTION\"",
            "",
            "echo \"✅ .env updated. Current values:\"",
            "grep -E \"VITE_API_BASE_URL|VITE_WEBSOCKET_URL|VITE_WEBSOCKET_URL_VOICEOPS|VITE_TRANSCRIBE_API_URL|VITE_STACK_SELECTION\" \"$ENV_FILE\"",
            "",
            "# 🚧 Build the app",
            "echo \"⚙️ Running npm run build...\"",
            "npm run build",
            "",
            "# ☁️ Clean and upload to S3 bucket root",
            "echo \"🧹 Clearing existing files in s3://${BUCKET_NAME}/ ...\"",
            "aws s3 rm \"s3://${BUCKET_NAME}/\" --recursive --region \"$REGION\"",
            "echo \"☁️ Uploading dist/ contents to s3://${BUCKET_NAME}/ ...\"",
            "aws s3 cp dist/ \"s3://${BUCKET_NAME}/\" --recursive --region \"$REGION\"",
            "echo \"✅ Done! React app built and uploaded to s3://${BUCKET_NAME}/\"",
            "TOKEN=$(curl -s -X PUT \"http://169.254.169.254/latest/api/token\" -H \"X-aws-ec2-metadata-token-ttl-seconds: 21600\")",
            "INSTANCE_ID=$(curl -s -H \"X-aws-ec2-metadata-token: $TOKEN\" http://169.254.169.254/latest/meta-data/instance-id)",
            "aws ec2 terminate-instances --instance-ids \"$INSTANCE_ID\" --region \"$REGION\""
            
           
        )
