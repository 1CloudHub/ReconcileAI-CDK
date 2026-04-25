# ReconcileAI No SAP CDK Deployment Guide

## Overview

ReconcileAI is an AI-powered document reconciliation platform designed to simplify and automate the comparison of business documents across complex workflows. It enables intelligent field extraction, configurable matching rules, and SOP-driven exception handling to process and validate information across multiple document types such as invoices, statements, reports, transaction records, and operational documents.

Built with an AI-first architecture, ReconcileAI supports scalable reconciliation workflows, enabling organizations to handle increasing document volumes while maintaining transparency, traceability, and performance insights through integrated monitoring and analytics.

---

## Pre-Deployment Steps

### Login to the AWS Console

Log in to the provided AWS account using IAM credentials or SSO access, based on the shared onboarding instructions.

---

## Deployment Steps

### 1. Set AWS Region in your aws cloud console

Navigate to the AWS Console region selector and ensure one of the following regions is selected:

<img width="2558" height="1382" alt="aws console region step 1" src="https://github.com/user-attachments/assets/2ac1d6de-14c8-4804-8623-17fc768931fc" />


> **Note:** Ensure you have access to Claude models or Nova Models in the selected region. If access is unavailable, switch to another region where Claude or Nova models are supported for your account, or contact Amazon Web Services support for assistance.

---

### 2. Open AWS CloudShell

Launch the AWS CloudShell service from the AWS Console.

CloudShell provides a pre-configured environment with AWS CLI and CDK support, making it ideal for deployments.

<img width="1897" height="898" alt="cloudshell pic step 2" src="https://github.com/user-attachments/assets/8dc0c7d5-d4cc-44b0-bdc3-9419e1faac82" />

---

### 3. Clone the Repository

In the terminal now we need to run these commands.

```bash
git clone --depth 1 https://github.com/1CloudHub/ReconcileAI-CDK.git
```

```bash
cd ReconcileAI-CDK
```

Clones the ReconcileAI NoSap CDK repository into your CloudShell environment.

---

### 4. Install Python Requirements

```bash
pip install -r requirements.txt
```
Installs the required Python dependencies for the CDK application.

> ⚠️ **Error Handling Only — Do Not Run Unless Needed**

<img width="1901" height="528" alt="delete_terminal step 3" src="https://github.com/user-attachments/assets/d3822f34-f02b-486a-b84b-f487b3054462" />



If you encounter memory or disk space issues in CloudShell:

1. Click the **Actions** menu at the top-right of the CloudShell terminal.
2. Select **Delete** to remove the current environment.
3. Open a new CloudShell terminal.
4. Restart from Step 1.

> **Important:** Only follow these steps if you receive an *insufficient space* or memory-related error during installation.

---

### 5. Install AWS CDK CLI

```bash
sudo npm install -g aws-cdk
```

Installs the AWS CDK CLI globally inside CloudShell.

---

### 6. Bootstrap CDK

```bash
cdk bootstrap
```

Prepares your AWS environment for CDK deployments by provisioning required toolkit resources.

If you encounter a **No storage available** error, run:

```bash
rm -rf ~/.local/bin/qchat ~/.local/bin/q ~/.local/bin/qterm
```

Then retry:

```bash
cdk bootstrap
```

---

### 7. Deploy the Stack

```bash
python deploy.py
```

Runs the deployment script and provisions the required AWS infrastructure for ReconcileAI.

#### 7.1 Model Selection

Once the CDK has been successfully bootstrapped, you will be prompted to choose the model required for the application.

Available options include:

- **Nova Premier**
- **Claude Sonnet 4**

<img width="1902" height="1113" alt="Screenshot 2026-04-22 182753" src="https://github.com/user-attachments/assets/5ad3107c-f7cd-4d79-aa1e-0d52bb2c11a7" />

Pick the model that matches the permissions and features enabled in your AWS account.

> **Note:** When selecting Claude Sonnet models, ensure you already have access to those models in the chosen region. If access is unavailable, contact Amazon Web Services support or switch to a region where the models are supported, then repeat the setup steps.

---

#### 7.2 Setting Token Limits

<img width="2417" height="430" alt="image" src="https://github.com/user-attachments/assets/5de88555-2399-4465-a10b-ae55907b78b8" />

Choose the specific token limits you want to configure for the application.
These limits define how much tokens the selected AI model can process for the application, helping control performance, usage, and cost in your aws account.

---

### 8. Confirm Deployment

Once the model and token limit are set, type yes and hit enter to confirm the deployment of resources into your AWS Account.

<img width="1646" height="886" alt="image" src="https://github.com/user-attachments/assets/ad27dcbb-afba-45df-b3ad-ab640287e7f7" />

---
### 9. Get the Application Url

To access the application, navigate to the AWS CloudFormation console and make sure you are viewing the same AWS region where the CDK resources were deployed.

<img width="2166" height="857" alt="CloudFormation Region Selection" src="https://github.com/user-attachments/assets/d2b6e2d0-737b-4c39-9637-8a48a2eea3a3" />

Once inside CloudFormation, you should see two deployed stacks.

<img width="1226" height="594" alt="CloudFormation Stacks" src="https://github.com/user-attachments/assets/cc1e8d0e-48d6-483f-a4e9-a654735c4430" />

Select the `ReconcileaiNoSapCdkStack` stack and navigate to the **Outputs** tab.

<img width="2640" height="1180" alt="CloudFormation Outputs Tab" src="https://github.com/user-attachments/assets/f8098b49-84b6-43c2-9e59-794c41f236ec" />

The **Outputs** section contains deployment-generated values such as the frontend URL, login credentials, and other important resource details.

---
### 10. Accessing the Application

Once the CloudFront distribution is active and model access has been approved, open the copied domain URL in your browser to start using the ReconcileAI application.

> **Note:** Ensure the deployment has completed successfully before attempting to access the application.

⚠️ **Deployment Time:**  
Deployment typically takes **20–30 minutes**. After deployment completes, allow an additional **10 minutes** for all services to fully initialize before using the application.

---
## Used AWS Services

Below is the list of AWS resources utilized by the ReconcileAI application.

```bash
1. Networking
   1.1 VPC with public and private subnets
   1.2 Security groups for Lambda, RDS, and EC2
   1.3 RDS subnet group across private subnets

2. Storage
   2.1 Frontend S3 bucket for website hosting
   2.2 Application S3 bucket for uploads and document storage
   2.3 Pre-created folders for reconciliation workflows and SOPs

3. Database
   3.1 PostgreSQL RDS instance running in private subnets
   3.2 Encrypted storage with restricted public access

4. Secrets Management
   4.1 Centralized secret storing database credentials
   4.2 Stores Bedrock model configuration and AWS region settings
   4.3 Includes Cognito IDs and application configuration values

5. Authentication
   5.1 Cognito User Pool for login management
   5.2 Email-based authentication
   5.3 Auto-created default user during deployment

6. Lambda Functions
   6.1 Database initialization Lambda
   6.2 Configuration Lambda
   6.3 Agent Lambda for reconciliation processing
   6.4 Shared Lambda layers for dependencies

7. IAM Roles
   7.1 Dedicated roles for Lambda, EC2, API Gateway, and S3 access
   7.2 Permissions configured for Bedrock, Cognito, Textract, RDS, and S3

8. API Gateway
   8.1 REST API for application endpoints
   8.2 Routes for configuration, uploads, and reconciliation
   8.3 CORS enabled for frontend integration

9. CloudFront
   9.1 CDN distribution for frontend delivery
   9.2 HTTPS enabled for secure access
   9.3 Automatic cache invalidation on deployment

10. EC2
    10.1 Frontend build instance
    10.2 Builds and uploads frontend assets automatically
    10.3 Self-terminates after deployment

11. Custom Resources
    11.1 Cognito user creation
    11.2 Database schema initialization
    11.3 S3 folder creation
    11.4 CloudFront cache invalidation

12. Outputs
    12.1 CloudFront frontend URL
    12.2 Default login credentials
    12.3 Deployment-generated access details

```

---
## Test ReconcileAI with Documents

After deployment, you can validate the application by uploading your own business documents and by running a reconciliation process to verify functionality and results. Happy Reconciling!!

Or you can also use the sample documents from below to get started quickly:

Three way matching documents: 

- [Goods Receipt Note — GRN 5100011111 (1).pdf](https://github.com/user-attachments/files/26970755/Goods.Receipt.Note.GRN.5100011111.1.pdf)
- [Purchase Order — PO 4500012345.pdf](https://github.com/user-attachments/files/26970776/Purchase.Order.PO.4500012345.pdf)
- [Tax Invoice — INV 90007777.pdf](https://github.com/user-attachments/files/26970782/Tax.Invoice.INV.90007777.pdf)

SOP Documents: 
- [SOP_UOM_ISSUE_V1 (1).pdf](https://github.com/1CloudHub/ReconcileAI-CDK/files//SOP_UOM_ISSUE_V1.1.pdf)
- [SOP_QUANTITY_CHANGE_V1 (1).pdf](https://github.com/user-attachments/files/26970803/SOP_QUANTITY_CHANGE_V1.1.pdf)
- [SOP_PRICE_CHANGE_V1 (2).pdf](https://github.com/user-attachments/files/26970798/SOP_PRICE_CHANGE_V1.2.pdf)
- [SOP_PO_GR_SI_RECONCILIATION_V2.pdf](https://github.com/user-attachments/files/27078664/SOP_PO_GR_SI_RECONCILIATION_V2.pdf)

---
## About ReconcileAI

ReconcileAI streamlines document reconciliation by enabling intelligent field extraction, configurable matching logic, and SOP-driven exception handling across multiple business documents. The platform reduces manual validation effort by automating dynamic document matching workflows, helping teams identify inconsistencies faster and improving operational accuracy.

---
## Legal Notice

© **1CloudHub. All rights reserved.**

The materials and components contained in this project are provided for demonstration purposes only.

No portion of this project may be implemented in a live or production environment without prior technical assessment, security clearance, and explicit approval from **1CloudHub**.

