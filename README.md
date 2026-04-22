# ReconcileAI No SAP CDK Deployment Guide

## Overview

ReconcileAI is an AI-powered web platform that helps users manually create and configure document types, create reconciliation jobs, and compare multi-document transactions such as invoices, purchase orders, and delivery notes using agent-managed three-way matching — without requiring ERP integration.

It allows users to define extraction rules, create SOP-based exception handling workflows, upload documents, and run an AI agent that matches fields across documents to detect inconsistencies.

> **Disclaimer:** This CDK setup is strictly designed and tested for the `us-east-1` region (N. Virginia) and `us-west-2` (Oregon). Please ensure that all resources are deployed only within these regions to avoid compatibility issues.

---

## Prerequisites

Before beginning the deployment process:

- Ensure you have access to the correct AWS account.
- You must be using either:
  - `us-east-1` (US East - N. Virginia)
  - `us-west-2` (US West - Oregon)

---

## Pre-Deployment Steps

### Login to the AWS Console

Log in to the provided AWS account using IAM credentials or SSO access, based on the shared onboarding instructions.

---

## Deployment Steps

### 1. Set AWS Region to `us-east-1` or `us-west-2`

Navigate to the AWS Console region selector and ensure one of the following regions is selected:

- **US East (N. Virginia)** — `us-east-1`
- **US West (Oregon)** — `us-west-2`

<img width="2558" height="1382" alt="aws console region step 1" src="https://github.com/user-attachments/assets/56c13415-9a1b-4700-8458-a15516fa9b99" />

This is critical, as all the CDK resources are scoped and supported only in these region

---

### 2. Open AWS CloudShell

Launch the AWS CloudShell service from the AWS Console.

CloudShell provides a pre-configured environment with AWS CLI and CDK support, making it ideal for deployments.

<img width="1897" height="898" alt="cloudshell pic step 2" src="https://github.com/user-attachments/assets/99f3e5c7-f6d1-4cb3-9906-93aa0cf8a277" />

CloudShell provides a pre-configured environment with AWS CLI and CDK support, making it ideal for deployments.
---

### 3. Clone the Repository

```bash
git clone https://github.com/1CloudHub/ReconcileAI-CDK.git
```

```bash
cd ReconcileAI-CDK
```

Clones the ReconcileAI NoSap CDK repository into your CloudShell environment.

---

### 4. Install Python Requirements

```bash
pip install --user -r requirements.txt
```
Installs the required Python dependencies for the CDK application.

> ⚠️ **Error Handling Only — Do Not Run Unless Needed**

<img width="1901" height="528" alt="delete_terminal step 3" src="https://github.com/user-attachments/assets/d85daa65-4cdf-420b-b603-4944f0994580" />


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

Select the model based on your application's AI processing requirements.

---

#### 7.2 Setting Token Limits

Choose the specific token limits you want to configure for the application.

These limits define how many tokens the selected AI model can process per request or session, helping control performance, usage, and cost.
