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

---

### 3. Clone the Repository

In the command line now we need to run these commands.

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


Select the model based on your application's AI processing requirements.

---

#### 7.2 Setting Token Limits

<img width="2417" height="430" alt="image" src="https://github.com/user-attachments/assets/5de88555-2399-4465-a10b-ae55907b78b8" />


Choose the specific token limits you want to configure for the application.

These limits define how many tokens the selected AI model can process per request or session, helping control performance, usage, and cost.

## Sample Documents for Testing

To validate the application after deployment, you can upload sample business documents and run a reconciliation session.

Download or view the testing documents below:

Three way matching documents: 

- [Goods Receipt Note — GRN 5100011111 (1).pdf](https://github.com/user-attachments/files/26970755/Goods.Receipt.Note.GRN.5100011111.1.pdf)
- [Purchase Order — PO 4500012345.pdf](https://github.com/user-attachments/files/26970776/Purchase.Order.PO.4500012345.pdf)
- [Tax Invoice — INV 90007777.pdf](https://github.com/user-attachments/files/26970782/Tax.Invoice.INV.90007777.pdf)

SOP Documents: 
- [SOP_MISSING_GRN_V1 (1).pdf](https://github.com/user-attachments/files/26970810/SOP_MISSING_GRN_V1.1.pdf)
- [SOP_DUPLICATE_INVOICE_V1 (1).pdf](https://github.com/user-attachments/files/26970807/SOP_DUPLICATE_INVOICE_V1.1.pdf)
- [SOP_UOM_ISSUE_V1 (1).pdf](https://github.com/1CloudHub/ReconcileAI-CDK/files//SOP_UOM_ISSUE_V1.1.pdf)
- [SOP_QUANTITY_CHANGE_V1 (1).pdf](https://github.com/user-attachments/files/26970803/SOP_QUANTITY_CHANGE_V1.1.pdf)
- [SOP_PRICE_CHANGE_V1 (2).pdf](https://github.com/user-attachments/files/26970798/SOP_PRICE_CHANGE_V1.2.pdf)

# End-to-End Configuration & Testing Workflow

Follow the steps below to configure document extraction, create reconciliation logic, and test a complete reconciliation workflow inside ReconcileAI.

---

### 1. Configure Document Types

Navigate to the **Document Types** section to define the documents that the AI agent will process.

For each document category:

- Create a new document type
- Define the fields you want the AI to extract
- Upload a sample document to validate extraction accuracy
- Save the configuration

#### Recommended Document Types

Create the following document types:

##### Goods Receipt
Configure fields such as:

- Goods Receipt Number
- Purchase Order Number
- Vendor Name
- Delivery Date
- Quantity
- Material Description

##### Purchase Order
Configure fields such as:

- Purchase Order Number
- Vendor Name
- Order Date
- Total Amount
- Currency
- Item Quantity

##### Sales Invoice
Configure fields such as:

- Invoice Number
- Purchase Order Reference
- Invoice Date
- Vendor Name
- Total Amount
- Tax Amount

#### Validate Extraction (IDP Testing)

After defining fields:

1. Upload a test document.
2. Run extraction validation.
3. Verify that the Intelligent Document Processing (IDP) correctly detects and maps fields.
4. Adjust field names if extraction results are inconsistent.

Repeat this process for all document types.

---

### 2. Create a Reconciliation Job

Navigate to the **Job Configuration** tab.

Jobs define which document types should be processed together.

#### Example: Three-Way Invoice Matching

Create a job that includes:

- Purchase Order
- Goods Receipt
- Sales Invoice

#### Steps

1. Create a new job.
2. Add the required document types.
3. Select the reference document if applicable.
4. Save the job configuration.

This job becomes the processing group used during reconciliation.

---

### 3. Configure SOP Rules

Navigate to the **SOP Configuration** section.

SOPs define the business rules and exception logic used during reconciliation.

### Steps

1. Create a new SOP.
2. Select the job that the SOP will apply to.
3. Upload an SOP document to act as the baseline logic.
4. Optionally upload the same SOP document for testing purposes.
5. Save the SOP configuration.

#### Example SOP Logic

The SOP may define rules such as:

- PO Number must match across all documents
- Quantity variance should not exceed allowed limits
- Invoice amount must align with Purchase Order totals
- Vendor name must remain consistent

---

### 4. Run the Reconciliation Agent

Navigate to the **Reconcile Agent** section.

#### Steps

1. Select the configured job.
2. Upload the related documents.
3. Assign document types if required.
4. Trigger the reconciliation process.

The AI agent will:

- Extract configured fields
- Compare documents
- Apply SOP logic
- Detect mismatches and exceptions

---

### 5. Review Reconciliation Sessions

Navigate to the **Reconcile Sessions** page.

Here you can:

- View reconciliation history
- Inspect processed documents
- Review field-level matching results
- Identify exceptions
- Analyze token consumption
- Open detailed session records

#### Session Detail View Includes

- Side-by-side document comparison
- Matching and mismatched fields
- SOP validation outcomes
- Processing status
- Token usage metrics

---

### 6. View Dashboard Metrics

Navigate to the **Dashboard** section.

This dashboard gives a centralized view of overall reconciliation activity and system usage.

---
# About ReconcileAI No SAP

ReconcileAI streamlines document reconciliation by enabling intelligent field extraction, configurable matching logic, and SOP-driven exception handling across multiple business documents.

The platform reduces manual validation effort by automating three-way matching workflows, helping teams identify inconsistencies faster and improve operational accuracy without relying on ERP integrations.

Built with an AI-first approach, ReconcileAI centralizes reconciliation, monitoring, and analytics into a single workflow-driven platform.

---

## Legal Notice

© **1CloudHub. All rights reserved.**

The materials and components contained in this project are provided for demonstration purposes only.

No portion of this project may be implemented in a live or production environment without prior technical assessment, security clearance, and explicit approval from **1CloudHub**.

