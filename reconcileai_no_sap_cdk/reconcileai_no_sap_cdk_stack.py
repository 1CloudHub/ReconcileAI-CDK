from aws_cdk import (
    # Duration,
    Stack,
    aws_ec2 as ec2   
)
from constructs import Construct
import random
import string


reconcile_name = "reconcileai_no_sap"

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

unique_key = generate_random_alphanumeric(8)

class ReconcileaiNoSapCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # vpc creation
        vpc = ec2.Vpc(
            self, reconcile_name + "VPC" + unique_key,
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
            self, "MyDBSubnetGroup",
            description="Subnet group for RDS database",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            )
        )  



