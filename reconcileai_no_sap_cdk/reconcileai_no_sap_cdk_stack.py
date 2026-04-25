from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    Size,
    Fn,
    SecretValue,  
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_secretsmanager as secretsmanager,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
)
from constructs import Construct
import random
import string
import json
import time

from aws_cdk import custom_resources as cr
from aws_cdk import aws_apigateway as apigw
from aws_cdk import aws_cognito as cognito
from aws_cdk import CfnOutput



reconcile_name = "reconcileai_no_sap"

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

name_key = generate_random_alphanumeric(8)
lambda_safe_key = generate_lambda_safe_name()
rds_safe_key = generate_rds_safe_name()

class ReconcileaiNoSapCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, bedrock_model_id: str = "us.amazon.nova-pro-v1:0",
        token_limit: int = 20000, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # vpc creation
        vpc = ec2.Vpc(
            self, reconcile_name + "VPC" + name_key,
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
            ],
            restrict_default_security_group=False,
        )

        # security group creation
        lambda_security_group = ec2.SecurityGroup(
            self, "LambdaSecurityGroup",
            vpc=vpc,
            description="Security group for Lambda functions",
            allow_all_outbound=True
        )

        rds_security_group = ec2.SecurityGroup(
            self, "RDSSecurityGroup",
            vpc=vpc,
            description="Security group for RDS instance",
            allow_all_outbound=False
        )

        rds_security_group.add_ingress_rule(
            peer=lambda_security_group,
            connection=ec2.Port.tcp(5432),
            description="Allow PostgreSQL access from Lambda"
        )


        lambda_security_group.add_egress_rule(
            peer=rds_security_group,
            connection=ec2.Port.tcp(5432),
            description="Allow Lambda to connect to RDS"
        )

        # Create RDS subnet group
        db_subnet_group = rds.SubnetGroup(
            self, "ReconcileaiDBSubnetGroup"+name_key,
            description="Subnet group for RDS database",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            )
        )

        s3_bucket_name = "reconcileai"+name_key
        frontend_bucket_name = "reconcileai-front"+name_key

        frontend_bucket = s3.Bucket(
            self, 
            "ReconcileaiFrontendBucket"+name_key,
            bucket_name=frontend_bucket_name,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,  # For development only
            auto_delete_objects=True,  # For development only
            website_index_document="index.html",
            website_error_document="index.html",
            # public_read_access=True,  # Allow public read access
            # block_public_access=s3.BlockPublicAccess.BLOCK_NONE  # Disable public access blocking
            cors=[
                s3.CorsRule(
                    allowed_methods=[
                        s3.HttpMethods.GET,
                        s3.HttpMethods.PUT,
                        s3.HttpMethods.POST,
                        s3.HttpMethods.HEAD,
                    ],
                    allowed_origins=["*"],  # ✅ safest for your setup
                    allowed_headers=["*"],
                    exposed_headers=["ETag"],
                    max_age=3000,
                )
            ],
        )

        frontend_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowAmplifyReadAccess",
                effect=iam.Effect.ALLOW,
                principals=[iam.ServicePrincipal("amplify.amazonaws.com")],
                actions=[
                    "s3:GetObject",
                ],
                resources=[frontend_bucket.bucket_arn + "/*"],
            )
        )

        frontend_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowAmplifyListAccess",
                effect=iam.Effect.ALLOW,
                principals=[iam.ServicePrincipal("amplify.amazonaws.com")],
                actions=[
                    "s3:ListBucket",
                ],
                resources=[frontend_bucket.bucket_arn],
            )
        )

        frontend_deploy = s3deploy.BucketDeployment(
            self,
            "ReconcileaiDeployFrontendFolder"+name_key,
            sources=[s3deploy.Source.asset("frontend")],  # Path to your frontend folder
            destination_bucket=frontend_bucket,
            destination_key_prefix="",  # Upload to root of bucket
        )


        # ── Main App S3 Bucket (with folder structure) ────────────────────────────
        s3_bucket = s3.Bucket(
            self,
            "ReconcileaiAppBucket" + name_key,
            bucket_name=s3_bucket_name,           # "reconcileai" + name_key  (already defined above)
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,  # Change to RETAIN for prod
            auto_delete_objects=True,
            cors=[
                s3.CorsRule(
                    allowed_methods=[
                        s3.HttpMethods.GET,
                        s3.HttpMethods.PUT,
                        s3.HttpMethods.POST,
                        s3.HttpMethods.HEAD,
                    ],
                    allowed_origins=["*"],  # ✅ safest for your setup
                    allowed_headers=["*"],
                    exposed_headers=["ETag"],
                    max_age=3000,
                )
            ],
        )

        # ── Create the 4 folders inside App S3 Bucket ────────────────────────────
        # S3 folders are just empty objects with a trailing slash as the key.
        # Using AwsCustomResource to create placeholder objects.

        folders = ["era_demo", "reconcile_agent", "sop_documents", "sop_test"]

        for folder in folders:
            cr.AwsCustomResource(
                self,
                f"Create{folder.replace('_', '').capitalize()}Folder" + name_key,
                on_create=cr.AwsSdkCall(
                    service="S3",
                    action="putObject",
                    parameters={
                        "Bucket": s3_bucket.bucket_name,
                        "Key": f"{folder}/",
                        "Body": "",
                    },
                    physical_resource_id=cr.PhysicalResourceId.of(
                        f"{s3_bucket.bucket_name}/{folder}/"
                    ),
                ),
                policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                    resources=[s3_bucket.bucket_arn + "/*"]
                ),
            )

        # ── Upload SQL schema file to app S3 bucket ───────────────────────────
        sql_deploy = s3deploy.BucketDeployment(
            self,
            reconcile_name + "SqlDeploy" + name_key,
            sources=[s3deploy.Source.asset("sql")],   # sql/ folder containing init_schema.sql
            destination_bucket=s3_bucket,
            destination_key_prefix="db_init",         # uploads to s3://bucket/db_init/init_schema.sql
        )

        # ── Agent Lambda Role ─────────────────────────────────────────────────────
        agent_lambda_role = iam.Role(
            self,
            "ReconcileaiAgentLambdaRole" + name_key,
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="IAM role for the Reconcile Agent Lambda",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonAPIGatewayAdministrator"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonAPIGatewayInvokeFullAccess"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonBedrockFullAccess"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonCognitoPowerUser"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonRDSFullAccess"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonS3FullAccess"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonTextractFullAccess"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaVPCAccessExecutionRole"
                ),
            ],
        )

        s3_doc_upload_role = iam.Role(
            self,
            reconcile_name + "S3DocUploadRole" + name_key,
            role_name="s3_doc_upload" + name_key,                          
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("lambda.amazonaws.com"),
                iam.ServicePrincipal("ec2.amazonaws.com"),
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess"),
            ],
        )

        s3_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowS3DocUploadRole" + name_key,
                effect=iam.Effect.ALLOW,
                principals=[
                    iam.ArnPrincipal(s3_doc_upload_role.role_arn),
                ],
                actions=["s3:*"],
                resources=[
                    s3_bucket.bucket_arn,
                    f"{s3_bucket.bucket_arn}/*",
                ],
            )
        )

        # ── Config Lambda Role ────────────────────────────────────────────────────
        config_lambda_role = iam.Role(
            self,
            "ReconcileaiConfigLambdaRole" + name_key,
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="IAM role for the Reconcile Config Lambda",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonBedrockFullAccess"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonCognitoPowerUser"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonS3FullAccess"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonTextractFullAccess"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaVPCAccessExecutionRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "SecretsManagerReadWrite"
                ),
            ],
        )

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

         # ── User Pool Client ──────────────────────────────────────────────────
        user_pool_client = cognito.UserPoolClient(
            self,
            reconcile_name + "UserPoolClient" + name_key,
            user_pool=user_pool,
            user_pool_client_name=reconcile_name + "-client-" + name_key,
            auth_flows=cognito.AuthFlow(
                user_password=True,
                user_srp=True,
                admin_user_password=True,
            ),
            generate_secret=False,
        )


        # ── RDS Instance ──────────────────────────────────────────────────────
        db_instance = rds.DatabaseInstance(
            self,
            reconcile_name + "RDSInstance" + rds_safe_key,
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_15
            ),
            instance_identifier=reconcile_name.replace("_", "-") + "-db-" + name_key,
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3,
                ec2.InstanceSize.MICRO,
            ),
            vpc=vpc,
            subnet_group=db_subnet_group,
            security_groups=[rds_security_group],
            # No generated secret — credentials live in app_secret above
            credentials=rds.Credentials.from_password(
                username="postgres",
                password=SecretValue.unsafe_plain_text("postgres123"),
            ),
            database_name="postgres",
            port=5432,
            multi_az=False,                         # set True for prod
            allocated_storage=20,
            storage_encrypted=True,
            deletion_protection=False,              # set True for prod
            removal_policy=RemovalPolicy.DESTROY,   # change to RETAIN for prod
            publicly_accessible=False,
        )

        # ── Secrets Manager ───────────────────────────────────────────────────
        # Single secret with ALL keys in one place — exactly as your existing
        # "reconcileai/dev/lambda-secrets" is structured.
        # db credentials are included here (no separate RDS secret).
        app_secret = secretsmanager.Secret(
            self,
            reconcile_name + "LambdaSecret" + name_key,
            secret_name="reconcileai/dev/lambda-secrets"+name_key,
            description="All config and credentials for ReconcileAI Lambda functions",
            secret_object_value={

                # ── Bedrock ───────────────────────────────────────────────────
                "BEDROCK_MODEL_ID":                        SecretValue.unsafe_plain_text(bedrock_model_id),

                # ── AWS Region ────────────────────────────────────────────────
                "REGION_AWS":                              SecretValue.unsafe_plain_text(self.region),

                # ── Database credentials (all in one secret) ──────────────────
                "db_host":                                 SecretValue.unsafe_plain_text(db_instance.db_instance_endpoint_address),
                "db_port":                                 SecretValue.unsafe_plain_text("5432"),
                "db_user":                                 SecretValue.unsafe_plain_text("postgres"),
                "db_password":                             SecretValue.unsafe_plain_text("postgres123"),
                "db_database":                             SecretValue.unsafe_plain_text("postgres"),

                # ── S3 ────────────────────────────────────────────────────────
                "bucket_name_no_sap":                      SecretValue.unsafe_plain_text(s3_bucket_name),

                # ── DB Schema & Table names ───────────────────────────────────
                "sop_table":                               SecretValue.unsafe_plain_text("sop_table"),
                "Textops_schema":                          SecretValue.unsafe_plain_text("erp_no_sap"),
                "erp_no_sap_schema":                       SecretValue.unsafe_plain_text("erp_no_sap"),
                "document_type_table":                     SecretValue.unsafe_plain_text("document_table"),
                "document_table":                          SecretValue.unsafe_plain_text("document_table"),
                "job_table":                               SecretValue.unsafe_plain_text("job_table"),
                "document_processing_table":               SecretValue.unsafe_plain_text("document_processing_table"),
                "prompt_metadata_table":                   SecretValue.unsafe_plain_text("prompt_metadata_table"),
                "ai_suggestion_table":                     SecretValue.unsafe_plain_text("ai_suggestion_table"),
                "temp_document_processing_table":          SecretValue.unsafe_plain_text("temp_document_processing_table"),
                "cexp_ocr_ai_key_extraction_details_table":SecretValue.unsafe_plain_text("cexp_ocr_ai_key_extraction_details_table"),

                # ── Cognito ───────────────────────────────────────────────────
                "COGNITO_USER_POOL_ID_NO_SAP":             SecretValue.unsafe_plain_text(user_pool.user_pool_id),
                "COGNITO_CLIENT_ID_NO_SAP":                SecretValue.unsafe_plain_text(user_pool_client.user_pool_client_id),
            },
        )

         # ── DB Schema Initializer Lambda ──────────────────────────────────────
        # A one-shot Lambda that runs inside the VPC, connects to RDS,
        # and executes the full schema SQL on first deploy.

        db_init_role = iam.Role(
            self,
            reconcile_name + "DbInitRole" + name_key,
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaVPCAccessExecutionRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
        )


        textract_layer = _lambda.LayerVersion(
            self,
            "TextractLayer",
            code=_lambda.Code.from_asset("lambda_layers/textractor_no_sap.zip"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            description="Textract processing layer which contains textractor"
        )

        psycopg2_layer = _lambda.LayerVersion(
            self,
            "Psycopg2Layer",
            code=_lambda.Code.from_asset("lambda_layers/psycopg_dateutil_only_no_sap.zip"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            description="Custom psycopg2 layer"
        )

        pymupdf_layer = _lambda.LayerVersion(
            self, 
            "PymupdfLayer",
            code = _lambda.Code.from_asset("lambda_layers/PymuPDF_no_sap.zip"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            description="Custom pymupdf layer"
        )

        db_init_lambda = _lambda.Function(
            self,
            reconcile_name + "DbInitLambda" + name_key,
            function_name=reconcile_name + "-db-init-" + name_key,
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="index.handler",
            role=db_init_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            security_groups=[lambda_security_group],
            timeout=Duration.seconds(300),
            memory_size=256,
            layers = [psycopg2_layer],
            code=_lambda.Code.from_asset("lambda/db_init_lambda"),
            environment={
                "DB_HOST":     db_instance.db_instance_endpoint_address,
                "DB_PORT":     "5432",
                "DB_NAME":     "postgres",
                "DB_USER":     "postgres",
                "DB_PASSWORD": "postgres123",
                "SQL_BUCKET":  s3_bucket_name,              
                "SQL_KEY":     "db_init/init_schema.sql",
                "TOKEN_LIMIT":  str(token_limit),
                "region_name":  self.region
            },
        )

        # Allow db_init_lambda to read the SQL file from S3
        s3_bucket.grant_read(db_init_lambda)

        # SQL must be uploaded before the init lambda runs
        db_init_lambda.node.add_dependency(sql_deploy)

        # Must wait for RDS to be ready before running
        db_init_lambda.node.add_dependency(db_instance)

        # ── Trigger the init Lambda once via CloudFormation custom resource ───
        db_schema_init = cr.AwsCustomResource(
            self,
            reconcile_name + "DbSchemaInit" + name_key,
            on_create=cr.AwsSdkCall(
                service="Lambda",
                action="invoke",
                parameters={
                    "FunctionName": db_init_lambda.function_name,
                    "InvocationType": "RequestResponse",
                    "Payload": json.dumps({
                        "RequestType": "Create",
                        "StackId":             "schema-init",
                        "RequestId":           "schema-init-001",
                        "LogicalResourceId":   "DbSchemaInit",
                        "ResponseURL":         "https://httpbin.org/put",
                        "TokenLimit":        token_limit,
                    }),
                },
                physical_resource_id=cr.PhysicalResourceId.of(
                    reconcile_name + "-db-schema-init-" + name_key
                ),
            ),
            policy=cr.AwsCustomResourcePolicy.from_statements([
                iam.PolicyStatement(
                    actions=["lambda:InvokeFunction"],
                    resources=[db_init_lambda.function_arn],
                )
            ]),
        )

        db_schema_init.node.add_dependency(db_init_lambda)
        db_schema_init.node.add_dependency(sql_deploy)


        
        # ── Config Lambda ─────────────────────────────────────────────────────
        # Points to the config_lambda/ subfolder which contains:
        # lambda_function.py, utils.py, db.py, helpers.py, job_config.py,
        # doc_config.py, sop_config.py, agent_records.py, dashboard.py
        config_lambda = _lambda.Function(
            self,
            reconcile_name + "ConfigLambda" + lambda_safe_key,
            function_name=reconcile_name + "-config-" + name_key,
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda/config_lambda"),   # ← config_lambda/ folder
            role=config_lambda_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            security_groups=[lambda_security_group],
            timeout=Duration.seconds(900),
            memory_size=1024,
            layers=[textract_layer, pymupdf_layer, psycopg2_layer],
            environment={
                "SECRET_NAME": app_secret.secret_name,
                "region_name": self.region,
                "AWS_REGION_NAME": self.region,
            },
        )

        # ── Agent Lambda ──────────────────────────────────────────────────────
        # Points to the lambda/ folder which has only reconcile_agent.py
        agent_lambda = _lambda.Function(
            self,
            reconcile_name + "AgentLambda" + lambda_safe_key,
            function_name=reconcile_name + "-agent-" + name_key,
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda/agent_lambda"),          # ← lambda/ folder (single file)
            role=agent_lambda_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            security_groups=[lambda_security_group],
            timeout=Duration.seconds(900),
            memory_size=2048,
            layers=[textract_layer, psycopg2_layer, pymupdf_layer],
            environment={
                "SECRET_NAME": app_secret.secret_name,
                "AWS_REGION_NAME": self.region,
                "region_name": self.region,
                # reconcile_agent.py reads these directly from os.environ
                "db_host":     db_instance.db_instance_endpoint_address,
                "db_port":     "5432",
                "db_database": "postgres",
                "db_user":     "postgres",
            },
        )

        # Grant both Lambdas read access to the secret
        app_secret.grant_read(config_lambda)
        app_secret.grant_read(agent_lambda)

        api = apigw.RestApi(
            self,
            reconcile_name + "ApiGateway" + name_key,
            rest_api_name=reconcile_name + "-api-" + name_key,
            description="ReconcileAI No-SAP API Gateway",
            deploy_options=apigw.StageOptions(
                stage_name="dev",
            ),
            # Enable default CORS for the whole API
            default_cors_preflight_options=apigw.CorsOptions(
                allow_origins=apigw.Cors.ALL_ORIGINS,
                allow_methods=apigw.Cors.ALL_METHODS,
                allow_headers=[
                    "Content-Type",
                    "X-Amz-Date",
                    "Authorization",
                    "X-Api-Key",
                    "X-Amz-Security-Token",
                ],
            ),
        )

        # ── Reusable CORS response parameters (applied to every method) ──────
        cors_response_parameters = {
            "method.response.header.Access-Control-Allow-Origin": True,
        }
        cors_integration_response_parameters = {
            "method.response.header.Access-Control-Allow-Origin": "'*'",
        }

        # ── Lambda integration settings (shared) ─────────────────────────────
        # proxy=False, buffered, passthrough — matches your screenshots exactly
        lambda_integration_options = {
            "proxy": False,
            "passthrough_behavior": apigw.PassthroughBehavior.WHEN_NO_TEMPLATES,
            "request_templates": {
                "application/json": "$input.json('$')"
            },
            "integration_responses": [
                apigw.IntegrationResponse(
                    status_code="200",
                    response_parameters=cors_integration_response_parameters,
                    response_templates={
                        "application/json": "$input.json('$')"
                    },
                )
            ],
        }

        method_response_200 = apigw.MethodResponse(
            status_code="200",
            response_parameters=cors_response_parameters,
            response_models={
                "application/json": apigw.Model.EMPTY_MODEL,
            },
        )

        # ────────────────────────────────────────────────────────────────────
        # /config resource
        # ────────────────────────────────────────────────────────────────────
        config_resource = api.root.add_resource("config")

        # /config  POST → config_lambda
        config_lambda_integration = apigw.LambdaIntegration(
            config_lambda,
            **lambda_integration_options,
        )
        config_resource.add_method(
            "POST",
            config_lambda_integration,
            method_responses=[method_response_200],
        )

        # /config  PUT → S3 direct integration
        # Path: reconcileai-no-sap-bucket/{filename}
        # URL path params: bucket (from querystring.bucket), filename (from querystring.filename)

        # IAM role to let API Gateway put objects into S3
        apigw_s3_role = iam.Role(
            self,
            reconcile_name + "ApiGatewayS3Role" + name_key,
            assumed_by=iam.ServicePrincipal("apigateway.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
            ],
            description="Allows API Gateway to PUT objects into the ReconcileAI S3 bucket",
        )
        s3_bucket.grant_put(apigw_s3_role)

        config_put_s3_integration = apigw.AwsIntegration(
            service="s3",
            integration_http_method="PUT",
            path=f"{s3_bucket_name}/{{filename}}",         # bucket/filename path
            options=apigw.IntegrationOptions(
                credentials_role=apigw_s3_role,
                passthrough_behavior=apigw.PassthroughBehavior.WHEN_NO_TEMPLATES,
                request_parameters={
                    # map querystring → URL path params for S3
                    "integration.request.path.bucket":   "method.request.querystring.bucket",
                    "integration.request.path.filename": "method.request.querystring.filename",
                },
                integration_responses=[
                    apigw.IntegrationResponse(
                        status_code="200",
                        response_parameters=cors_integration_response_parameters,
                    )
                ],
            ),
        )
        config_resource.add_method(
            "PUT",
            config_put_s3_integration,
            request_parameters={
                "method.request.querystring.bucket":   True,
                "method.request.querystring.filename": True,
            },
            method_responses=[method_response_200],
        )

        # ────────────────────────────────────────────────────────────────────
        # /reconcile-ai resource
        # ────────────────────────────────────────────────────────────────────
        reconcile_ai_resource = api.root.add_resource("reconcile-ai")

        # /reconcile-ai  POST → agent_lambda (proxy=False, no GET)
        reconcile_ai_lambda_integration = apigw.LambdaIntegration(
            agent_lambda,
            **lambda_integration_options,
        )
        reconcile_ai_resource.add_method(
            "POST",
            reconcile_ai_lambda_integration,
            method_responses=[method_response_200],
        )

        # ── Grant API Gateway permission to invoke both Lambdas ──────────────
        config_lambda.add_permission(
            "ApiGatewayInvokeConfigLambda",
            principal=iam.ServicePrincipal("apigateway.amazonaws.com"),
            source_arn=api.arn_for_execute_api(method="*", path="/*", stage="*"),
        )
        agent_lambda.add_permission(
            "ApiGatewayInvokeAgentLambda",
            principal=iam.ServicePrincipal("apigateway.amazonaws.com"),
            source_arn=api.arn_for_execute_api(method="*", path="/*", stage="*"),
        )


        # ── Strip /dev/ suffix from API URL ───────────────────────────────────
        api_base_url = Fn.select(0, Fn.split("/dev/", api.url))

        # ── EC2 Role ──────────────────────────────────────────────────────────
        ec2_role = iam.Role(
            self,
            reconcile_name + "EC2Role" + name_key,
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonEC2FullAccess"),
            ],
        )

        # ── EC2 Security Group ────────────────────────────────────────────────
        ec2_security_group = ec2.SecurityGroup(
            self,
            reconcile_name + "EC2SecurityGroup" + name_key,
            vpc=vpc,
            description="Security group for frontend build EC2",
            allow_all_outbound=True,
        )

        # ── Frontend Build EC2 Instance ───────────────────────────────────────
        # This instance:
        # 1. Downloads build.zip (source code) from S3
        # 2. Unzips it
        # 3. Updates VITE_RECONCILEAI_DEV_ENV_NO_SAP_BASE_URL in .env
        # 4. Runs npm run build
        # 5. Zips the dist/ output back as build.zip
        # 6. Uploads it back to the same S3 bucket (overwriting the source zip)
        # 7. Terminates itself
        ec2_instance_front = ec2.Instance(
            self,
            reconcile_name + "FrontendBuildEC2" + name_key,
            role=ec2_role,
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3,
                ec2.InstanceSize.MEDIUM,
            ),
            machine_image=ec2.MachineImage.latest_amazon_linux2023(),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC,
            ),
            security_group=ec2_security_group,
            user_data=ec2.UserData.for_linux(),
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",
                    volume=ec2.BlockDeviceVolume.ebs(
                        volume_size=30,
                        volume_type=ec2.EbsDeviceVolumeType.GP3,
                        delete_on_termination=True,
                        encrypted=True,
                    ),
                )
            ],
        )

        bucket_name_for_ec2 = frontend_bucket_name
        region_for_ec2 = self.region
        env_var_name = "VITE_RECONCILEAI_DEV_ENV_NO_SAP_BASE_URL"

        ec2_instance_front.add_user_data(
            "#!/bin/bash",
            "set -e",
            "echo '🚀 Starting frontend build process...'",
            "",
            "# ── Install prerequisites ─────────────────────────────────────────",
            "command_exists() { command -v \"$1\" &> /dev/null; }",
            "",
            "if ! command_exists unzip; then",
            "    sudo yum install -y unzip --allowerasing",
            "fi",
            "",
            "if ! command_exists node || ! command_exists npm; then",
            "    curl -fsSL https://rpm.nodesource.com/setup_20.x | sudo bash -",
            "    sudo yum install -y nodejs --allowerasing",
            "fi",
            "",
            "if ! command_exists aws; then",
            "    curl 'https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip' -o 'awscliv2.zip'",
            "    unzip awscliv2.zip",
            "    sudo ./aws/install",
            "    rm -rf aws awscliv2.zip",
            "fi",
            "",
            "# ── Set variables ─────────────────────────────────────────────────",
            f"BUCKET_NAME='{bucket_name_for_ec2}'",
            f"REGION='{region_for_ec2}'",
            f"ENV_VAR_NAME='{env_var_name}'",
            "WORK_DIR=~/react-app",
            "ZIP_FILE='build.zip'",
            f"S3_SOURCE_PATH=\"s3://${{BUCKET_NAME}}/${{ZIP_FILE}}\"",
            "",
            "# ── Get API Gateway base URL from the CDK token resolved at deploy ─",
            "# api_base_url is injected by CDK as a resolved CloudFormation value",
            "# We write it into a temp file during CDK synth via user_data string",
            f"API_URL='{api_base_url}'",
            "",
            "echo \"Using API URL: $API_URL\"",
            "",
            "# ── Download and unzip source code ───────────────────────────────",
            "mkdir -p \"$WORK_DIR\"",
            "cd \"$WORK_DIR\"",
            "echo '📥 Downloading source zip from S3...'",
            "aws s3 cp \"$S3_SOURCE_PATH\" . --region \"$REGION\"",
            "echo '📂 Unzipping...'",
            "unzip -o \"$ZIP_FILE\"",
            "rm \"$ZIP_FILE\"",
            "",
            "# ── Update .env with the real API Gateway URL ─────────────────────",
            "ENV_FILE='.env'",
            "touch \"$ENV_FILE\"",
            "echo '🛠 Updating .env...'",
            "if grep -q \"^$ENV_VAR_NAME=\" \"$ENV_FILE\"; then",
            "    sed -i \"s|^$ENV_VAR_NAME=.*|$ENV_VAR_NAME=$API_URL|\" \"$ENV_FILE\"",
            "else",
            "    echo \"$ENV_VAR_NAME=$API_URL\" >> \"$ENV_FILE\"",
            "fi",
            "",
            "echo '✅ .env updated:'",
            "cat \"$ENV_FILE\"",
            "",
            "# ── Install dependencies and build ────────────────────────────────",
            "echo '📦 Running npm install...'",
            "npm install",
            "npm install react-pdf",
            "echo '⚙️ Running npm run build...'",
            "npm run build",
            "",
            "# ☁️ Clean and upload to S3 bucket root",
            "echo \"🧹 Clearing existing files in s3://${BUCKET_NAME}/ ...\"",
            "aws s3 rm \"s3://${BUCKET_NAME}/\" --recursive --region \"$REGION\"",
            "echo \"☁️ Uploading dist/ contents to s3://${BUCKET_NAME}/ ...\"",
            "aws s3 cp dist/ \"s3://${BUCKET_NAME}/\" --recursive --region \"$REGION\"",
            "echo \"✅ Done! React app built and uploaded to s3://${BUCKET_NAME}/\"",
            "# ── Self-terminate ────────────────────────────────────────────────",
            "TOKEN=$(curl -s -X PUT 'http://169.254.169.254/latest/api/token' -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600')",
            "INSTANCE_ID=$(curl -s -H \"X-aws-ec2-metadata-token: $TOKEN\" http://169.254.169.254/latest/meta-data/instance-id)",
            "echo \"🛑 Terminating instance $INSTANCE_ID...\"",
            f"aws ec2 terminate-instances --instance-ids \"$INSTANCE_ID\" --region '{region_for_ec2}'",
        )

        # EC2 must run after the source zip is uploaded to S3
        ec2_instance_front.node.add_dependency(frontend_deploy)
        # EC2 must run after API Gateway is created (so api_base_url is resolved)
        ec2_instance_front.node.add_dependency(api)


        # Create CloudFront Distribution for frontend S3 bucket
        # Create S3 Origin using the new S3BucketOrigin (not deprecated)
        s3_origin = origins.S3BucketOrigin(
            frontend_bucket,
            origin_path=""  # Empty path means root of bucket
        )
 
        # Create CloudFront Distribution
        distribution = cloudfront.Distribution(
            self, "GenAIFoundryDistribution",
            default_behavior=cloudfront.BehaviorOptions(
                origin=s3_origin,
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                cache_policy=cloudfront.CachePolicy.CACHING_OPTIMIZED,
                origin_request_policy=None,
                response_headers_policy=None
            ),
            # General settings matching the console configuration
            default_root_object="index.html",
            price_class=cloudfront.PriceClass.PRICE_CLASS_ALL,
            http_version=cloudfront.HttpVersion.HTTP2,  # Fixed: use HTTP2 instead of HTTP2_AND_HTTP1_1
            enable_logging=False,  # Standard logging: Off
            enable_ipv6=True,
            # Error pages configuration matching the console
            error_responses=[
                cloudfront.ErrorResponse(
                    http_status=403,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.seconds(10)
                ),
                cloudfront.ErrorResponse(
                    http_status=404,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.seconds(10)
                )
            ]
        )

        # Switch to Origin Access Control (OAC) so S3 policy can use CloudFront service principal + AWS:SourceArn
        oac = cloudfront.CfnOriginAccessControl(
            self,
            "FrontendOAC",
            origin_access_control_config=cloudfront.CfnOriginAccessControl.OriginAccessControlConfigProperty(
                name=f"{name_key}-frontend-oac",
                description="OAC for frontend S3 origin",
                origin_access_control_origin_type="s3",
                signing_behavior="always",
                signing_protocol="sigv4",
            ),
        )
        cfn_dist = distribution.node.default_child  # type: ignore
        # Attach OAC to first origin and remove OAI reference
        cfn_dist.add_property_override(
            "DistributionConfig.Origins.0.OriginAccessControlId", oac.attr_id
        )
        cfn_dist.add_property_deletion_override(
            "DistributionConfig.Origins.0.S3OriginConfig.OriginAccessIdentity"
        )
        cfn_dist.add_dependency(oac)

        # Explicit CloudFront invalidation via AWS SDK (since L1 Invalidations are not available in this CDK version)
        invalidation = cr.AwsCustomResource(
            self,
            "GenAIFoundryInvalidation",
            on_update=cr.AwsSdkCall(
                service="CloudFront",
                action="createInvalidation",
                parameters={
                    "DistributionId": distribution.distribution_id,
                    "InvalidationBatch": {
                        "CallerReference": str(int(time.time())),
                        "Paths": {"Quantity": 1, "Items": ["/*"]},
                    },
                },
                physical_resource_id=cr.PhysicalResourceId.of(
                    f"InvalidateFrontend-{int(time.time())}"
                ),
            ),
            policy=cr.AwsCustomResourcePolicy.from_statements([
                iam.PolicyStatement(
                    actions=[
                        "cloudfront:CreateInvalidation",
                        "cloudfront:GetInvalidation",
                        "cloudfront:ListInvalidations",
                    ],
                    resources=["*"],
                )
            ]),
        )
        # Ensure invalidation runs after upload and distribution exist
        invalidation.node.add_dependency(frontend_deploy)
        invalidation.node.add_dependency(distribution)
        # Replace frontend bucket policy with the previously working policy
        # 1) Grant required S3 actions to account root
        frontend_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                principals=[
                    # Allow the entire account (root) to perform required actions
                    iam.ArnPrincipal(f"arn:aws:iam::{self.account}:root"),
                ],
                actions=[
                    "s3:DeleteObject*",
                    "s3:GetBucket*",
                    "s3:GetObject",
                    "s3:List*",
                    "s3:PutBucketPolicy"
                ],
                resources=[
                    frontend_bucket.bucket_arn,
                    f"{frontend_bucket.bucket_arn}/*"
                ]
            )
        )

        # 2) Allow CloudFront access to objects in the frontend bucket
        frontend_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowCloudFrontAccess",
                effect=iam.Effect.ALLOW,
                principals=[iam.ServicePrincipal("cloudfront.amazonaws.com")],
                actions=["s3:GetObject"],
                resources=[f"{frontend_bucket.bucket_arn}/*"],
                conditions={
                    "StringEquals": {
                        "AWS:SourceArn": distribution.distribution_arn
                    }
                }
            )
        )

        # Add bucket policy to main bucket to allow CloudFront access only
        s3_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowCloudFrontAccessOnly",
                effect=iam.Effect.ALLOW,
                principals=[iam.ServicePrincipal("cloudfront.amazonaws.com")],
                actions=["s3:GetObject"],
                resources=[f"{s3_bucket.bucket_arn}/*"],
                conditions={
                    "StringEquals": {
                        "AWS:SourceArn": distribution.distribution_arn
                    }
                }
            )
        )

        

        
 
        # Outputs for easy access to distribution information
        # CfnOutput(
        #     self, "DistributionDomainName",
        #     value=distribution.distribution_domain_name,
        #     description="CloudFront Distribution Domain Name"
        # )
       
        # CfnOutput(
        #     self, "DistributionId",
        #     value=distribution.distribution_id,
        #     description="CloudFront Distribution ID"
        # )
       
        # CfnOutput(
        #     self, "DistributionArn",
        #     value=distribution.distribution_arn,
        #     description="CloudFront Distribution ARN"
        # )

        CfnOutput(
            self, "CloudFrontDistributionUrl",
            value=f"https://{distribution.distribution_domain_name}",
            description="CloudFront Distribution URL for the frontend application"
        )
        
        CfnOutput(self, "LoginEmail", value=USER_EMAIL)
        CfnOutput(self, "LoginPassword", value=PASSWORD)