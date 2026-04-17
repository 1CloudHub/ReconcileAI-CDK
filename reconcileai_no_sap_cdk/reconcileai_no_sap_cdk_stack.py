from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    Size,
    SecretValue,  
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_secretsmanager as secretsmanager,
)
from constructs import Construct
import random
import string
from aws_cdk import custom_resources as cr
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_apigateway as apigw



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

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
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
            ]
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
                    ],
                    allowed_origins=["*"],
                    allowed_headers=["*"],
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
                # ── API ───────────────────────────────────────────────────────
                "API_GATEWAY_URL":                         SecretValue.unsafe_plain_text("https://your-api-id.execute-api.us-west-2.amazonaws.com/dev/reconcile-ai"),
                "FRONTEND_URL":                            SecretValue.unsafe_plain_text("https://your-frontend-url.com"),

                # ── Bedrock ───────────────────────────────────────────────────
                "BEDROCK_MODEL_ID":                        SecretValue.unsafe_plain_text("us.anthropic.claude-sonnet-4-20250514-v1:0"),

                # ── AWS Region ────────────────────────────────────────────────
                "REGION_AWS":                              SecretValue.unsafe_plain_text("us-west-2"),

                # ── Database credentials (all in one secret) ──────────────────
                "db_host":                                 SecretValue.unsafe_plain_text("PLACEHOLDER_REPLACE_AFTER_RDS_DEPLOY"),
                "db_port":                                 SecretValue.unsafe_plain_text("5432"),
                "db_user":                                 SecretValue.unsafe_plain_text("postgres"),
                "db_password":                             SecretValue.unsafe_plain_text("PLACEHOLDER_REPLACE_WITH_REAL_PASSWORD"),
                "db_database":                             SecretValue.unsafe_plain_text("postgres"),

                # ── S3 ────────────────────────────────────────────────────────
                "bucket_name_no_sap":                      SecretValue.unsafe_plain_text(s3_bucket_name),

                # ── DB Schema & Table names ───────────────────────────────────
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
            },
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
                password=SecretValue.unsafe_plain_text("PLACEHOLDER_REPLACE_WITH_REAL_PASSWORD"),
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


        boto3_layer = _lambda.LayerVersion(
            self,
            "Boto3Layer",
            code=_lambda.Code.from_asset("lambda_layers/boto3.zip"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],
            description="Custom boto3 layer"
        )

        mcp_layer = _lambda.LayerVersion(
            self,
            "McpLayer",
            code=_lambda.Code.from_asset("lambda_layers/mcp_v2.zip"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],
            description="MCP v2 layer"
        )

        textract_layer = _lambda.LayerVersion(
            self,
            "TextractLayer",
            code=_lambda.Code.from_asset("lambda_layers/textract_text_final.zip"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],
            description="Textract processing layer"
        )

        # ── Config Lambda ─────────────────────────────────────────────────────
        # Points to the config_lambda/ subfolder which contains:
        # lambda_function.py, utils.py, db.py, helpers.py, job_config.py,
        # doc_config.py, sop_config.py, agent_records.py, dashboard.py
        config_lambda = _lambda.Function(
            self,
            reconcile_name + "ConfigLambda" + lambda_safe_key,
            function_name=reconcile_name + "-config-" + name_key,
            runtime=_lambda.Runtime.PYTHON_3_11,
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
            layers=[boto3_layer, textract_layer],
            environment={
                "SECRET_NAME": "reconcileai/dev/lambda-secrets"+name_key,
                "AWS_REGION_NAME": self.region,
            },
        )

        # ── Agent Lambda ──────────────────────────────────────────────────────
        # Points to the lambda/ folder which has only reconcile_agent.py
        agent_lambda = _lambda.Function(
            self,
            reconcile_name + "AgentLambda" + lambda_safe_key,
            function_name=reconcile_name + "-agent-" + name_key,
            runtime=_lambda.Runtime.PYTHON_3_11,
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
            layers=[boto3_layer, mcp_layer],
            environment={
                "SECRET_NAME": "reconcileai/dev/lambda-secrets"+name_key,
                "AWS_REGION_NAME": self.region,
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
            source_arn=api.arn_for_execute_api("*", "*", "*"),
        )
        agent_lambda.add_permission(
            "ApiGatewayInvokeAgentLambda",
            principal=iam.ServicePrincipal("apigateway.amazonaws.com"),
            source_arn=api.arn_for_execute_api("*", "*", "*"),
        )









