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
            "service-role/AWSLambdaVPCAccessExecutionRole"       # ENIs for VPC access
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
        
        
        #REST API
        sap_api = apigateway.RestApi(
            self, "sapapigateway",
            rest_api_name="sap_rest_api",
            description="API Gateway for SAP data access",
            binary_media_types=["multipart/form-data"],
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=["*"],
                allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
                allow_headers=["*"],
                allow_credentials=False,
                max_age=Duration.seconds(86400)
                ),
            deploy_options=apigateway.StageOptions(
                stage_name="dev",
                logging_level=apigateway.MethodLoggingLevel.OFF,
                data_trace_enabled=False
                )
            )
        
        
        
        
        
        # Create WebSocket API Gateway using API Gateway v2
        # websocket_api = apigatewayv2.WebSocketApi(
        #     self, "SAPWebSocketAPI"+unique_key,
        #     api_name="SAP_ws"+unique_key
        # )

        # # Add routes to the WebSocket API
        # websocket_api.add_route(
        #     "$connect",
        #     integration=apigatewayv2_integrations.WebSocketLambdaIntegration(
        #         "ConnectIntegration",
        #         websocket_lambda_function
        #     )
        # )

        # websocket_api.add_route(
        #     "$disconnect",
        #     integration=apigatewayv2_integrations.WebSocketLambdaIntegration(
        #         "DisconnectIntegration",
        #         websocket_lambda_function
        #     )
        # )

        # websocket_api.add_route(
        #     "$default",
        #     integration=apigatewayv2_integrations.WebSocketLambdaIntegration(
        #         "DefaultIntegration",
        #         websocket_lambda_function
        #     )
        # )

        # # Create WebSocket Stage
        # websocket_stage = apigatewayv2.WebSocketStage(
        #     self, "WebSocketStage",
        #     web_socket_api=websocket_api,
        #     stage_name="dev",
        #     auto_deploy=True
        # )
        
        
        
        
        
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
    # "echo 'starting python code implementation'",
    # "export DEBIAN_FRONTEND=noninteractive",
    # "cd /home/ubuntu",
    # "aws s3 sync s3://sql-dumps-bucket/ec2_needs/ ./ec2_needs/",
    # "cd aivolvex-genai-foundry/ec2_needs",
    # "sudo apt install python3.10-venv -y",
    # "python3 -m venv eagle",
    # "source eagle/bin/activate",
    # "pip install -r requirements.txt --no-input",
    # "pip install asgiref --no-input",
    # "# Set environment variable and run in screen session",
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


"""
1. Dependencies installation
2. RDS connection and SQL dumps
3. Strands code insertion

last. Frontend management code from genaifoundry
"""      
        


