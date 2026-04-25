# ReconcileAI No SAP CDK Deployment Guide

## Overview

ReconcileAI streamlines document reconciliation by enabling intelligent field extraction, configurable matching logic, and SOP-driven exception handling across multiple business documents.

The platform reduces manual validation effort by automating dynamic document matching workflows, helping teams identify inconsistencies faster and improving operational accuracy.

Built with an AI-first approach, ReconcileAI centralizes reconciliation, monitoring, and analytics into a single workflow-driven platform.

---

## Pre-Deployment Steps

### Login to the AWS Console

Log in to the provided AWS account using IAM credentials or SSO access, based on the shared onboarding instructions.

---

## Deployment Steps

### 1. Set AWS Region to `us-east-1` or `us-west-2`

Navigate to the AWS Console region selector and ensure one of the following regions is selected:

<img width="2558" height="1382" alt="aws console region step 1" src="https://github.com/user-attachments/assets/56c13415-9a1b-4700-8458-a15516fa9b99" />

> **Note:** Ensure you have access to Claude models or Nova Models in the selected region. If access is unavailable, switch to another region where Claude or Nova models are supported for your account, or contact Amazon Web Services support for assistance.

---

### 2. Open AWS CloudShell

Launch the AWS CloudShell service from the AWS Console.

CloudShell provides a pre-configured environment with AWS CLI and CDK support, making it ideal for deployments.

<img width="1897" height="898" alt="cloudshell pic step 2" src="https://github.com/user-attachments/assets/99f3e5c7-f6d1-4cb3-9906-93aa0cf8a277" />

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

<img width="1556" height="903" alt="image" src="https://github.com/user-attachments/assets/4f5f9db6-b914-4d73-bd30-dd7d731d3545" />


Pick the model that matches the permissions and features enabled in your AWS account.

> **Note:** When selecting Claude Sonnet models, ensure you already have access to those models in the chosen region. If access is unavailable, contact Amazon Web Services support or switch to a region where the models are supported, then repeat the setup steps.

---

#### 7.2 Setting Token Limits

<img width="2417" height="430" alt="image" src="https://github.com/user-attachments/assets/5de88555-2399-4465-a10b-ae55907b78b8" />


Choose the specific token limits you want to configure for the application.

These limits define how much tokens the selected AI model can process for the application, helping control performance, usage, and cost in your aws accoutn.

## Test ReconcileAI with Documents

After deployment, you can validate the application by uploading your own business documents and by running a reconciliation process to verify functionality and results.

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

## Legal Notice

© **1CloudHub. All rights reserved.**

The materials and components contained in this project are provided for demonstration purposes only.

No portion of this project may be implemented in a live or production environment without prior technical assessment, security clearance, and explicit approval from **1CloudHub**.

