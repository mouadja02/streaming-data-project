# CI/CD Pipeline Setup Guide

This guide will help you set up automated deployment of your data pipeline infrastructure using GitHub Actions.

## Overview

When you push code to the `main` branch, the GitHub Actions workflow will automatically:

1. **Deploy Snowflake Infrastructure**
   - Create external stages pointing to your S3 bucket
   - Set up external tables for analytics
   - Configure file formats and permissions

2. **Deploy AWS Glue Jobs**
   - Create IAM roles with proper permissions
   - Upload Glue job scripts to S3
   - Create/update Glue jobs with Iceberg support
   - Set up Glue database and workflow

3. **Ready for Execution**
   - Jobs are deployed and ready to run in AWS Console
   - Snowflake tables are configured for analytics
   - Complete monitoring and logging setup

## Required GitHub Secrets

You need to configure these secrets in your GitHub repository:

### AWS Credentials
```
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
S3_BUCKET_NAME=your-s3-bucket-name
```

### Snowflake Credentials
```
SNOWFLAKE_USER=INTERNPROJECT
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_ACCOUNT=your_account-INTERNPROJECT
SNOWFLAKE_WAREHOUSE=INT_WH
SNOWFLAKE_DATABASE=ECOMMERCE_DB
SNOWFLAKE_SCHEMA=STAGING
```

## Setup Instructions

### Step 1: Configure GitHub Secrets

1. **Go to your GitHub repository**
2. **Navigate to Settings → Secrets and variables → Actions**
3. **Click "New repository secret"**
4. **Add each secret from the list above**

#### AWS Secrets Setup:
```bash
# Your AWS credentials (same as in .env file)
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=xyz123...
S3_BUCKET_NAME=my-amazing-app
```

#### Snowflake Secrets Setup:
```bash
# Your Snowflake credentials (same as in .env file)
SNOWFLAKE_USER=INTERNPROJECT
SNOWFLAKE_PASSWORD=your_password_here
SNOWFLAKE_ACCOUNT=your_account-INTERNPROJECT
SNOWFLAKE_WAREHOUSE=INT_WH
SNOWFLAKE_DATABASE=ECOMMERCE_DB
SNOWFLAKE_SCHEMA=STAGING
```

### Step 2: Verify AWS Permissions

Your AWS user/role needs these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:*",
        "iam:CreateRole",
        "iam:AttachRolePolicy",
        "iam:PutRolePolicy",
        "iam:GetRole",
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": "*"
    }
  ]
}
```

### Step 3: Configure S3 Bucket

Ensure your S3 bucket exists and has proper permissions:

```bash
# Create bucket if it doesn't exist
aws s3 mb s3://your-bucket-name

# Verify access
aws s3 ls s3://your-bucket-name
```

### Step 4: Test the Pipeline

1. **Make a small change** to any file in `glue_jobs/` or `snowflake/`
2. **Commit and push** to the `main` branch:
   ```bash
   git add .
   git commit -m "test: trigger CI/CD pipeline"
   git push origin main
   ```
3. **Monitor the workflow** in GitHub Actions tab
4. **Check AWS Glue Console** for deployed jobs
5. **Verify Snowflake** external tables

## What Gets Deployed

### AWS Infrastructure:
- **IAM Role**: `DataPipelineGlueRole` with Iceberg permissions
- **Glue Database**: `data_pipeline_db`
- **Glue Jobs**:
  - `data-pipeline-raw-transformation`
  - `data-pipeline-analytics-aggregation`
  - `data-pipeline-time-series-analysis`
- **Glue Workflow**: `data-pipeline-workflow`

### Snowflake Infrastructure:
- **External Stages**: Connected to your S3 bucket
- **External Tables**:
  - `ext_raw_users`
  - `ext_user_analytics`
  - `ext_user_demographics`
- **File Formats**: Optimized for Parquet with Snappy compression

## Running the Pipeline

After deployment, you can run the pipeline in several ways:

### Option 1: AWS Glue Console (Recommended)
1. Go to [AWS Glue Console](https://console.aws.amazon.com/glue/)
2. Navigate to **Jobs** section
3. Run jobs in sequence:
   - First: `data-pipeline-raw-transformation`
   - Then: `data-pipeline-analytics-aggregation`
   - Finally: `data-pipeline-time-series-analysis`

### Option 2: AWS CLI
```bash
# Run jobs sequentially
aws glue start-job-run --job-name data-pipeline-raw-transformation
aws glue start-job-run --job-name data-pipeline-analytics-aggregation
aws glue start-job-run --job-name data-pipeline-time-series-analysis
```

### Option 3: Glue Workflow
```bash
# Start the complete workflow
aws glue start-workflow-run --name data-pipeline-workflow
```

## Monitoring and Troubleshooting

### CloudWatch Logs
- **Log Groups**: `/aws-glue/jobs/output` and `/aws-glue/jobs/error`
- **Spark UI**: Enabled for job monitoring
- **Metrics**: Automatically collected

### Common Issues and Solutions

#### 1. **IAM Permission Errors**
```
Error: User: arn:aws:iam::123456789012:user/username is not authorized
```
**Solution**: Verify AWS user has Glue and IAM permissions

#### 2. **S3 Access Denied**
```
Error: Access Denied when accessing S3 bucket
```
**Solution**: Check S3 bucket permissions and AWS credentials

#### 3. **Snowflake Connection Failed**
```
Error: Failed to connect to Snowflake
```
**Solution**: Verify Snowflake credentials and network access

#### 4. **Glue Job Script Not Found**
```
Error: Script s3://bucket/glue-scripts/script.py not found
```
**Solution**: Ensure scripts uploaded correctly, check S3 bucket name

### Debugging Commands

```bash
# Check Glue job status
aws glue get-job-run --job-name data-pipeline-raw-transformation --run-id <run-id>

# List Glue jobs
aws glue get-jobs

# Check S3 scripts
aws s3 ls s3://your-bucket/glue-scripts/

# Test Snowflake connection
python snowflake_connector.py query
```

## Workflow Triggers

The pipeline automatically triggers on:

- **Push to main branch** with changes in:
  - `glue_jobs/**` (any Glue job script changes)
  - `snowflake/**` (any Snowflake configuration changes)
  - `.github/workflows/**` (workflow changes)

- **Pull Request** to main branch (validation only, no deployment)

## Additional Resources

### AWS Documentation
- [AWS Glue Developer Guide](https://docs.aws.amazon.com/glue/)
- [Apache Iceberg on AWS](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-iceberg.html)
- [AWS Glue IAM Roles](https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html)

### Snowflake Documentation
- [Snowflake External Tables](https://docs.snowflake.com/en/user-guide/tables-external-intro.html)
- [Snowflake S3 Integration](https://docs.snowflake.com/en/user-guide/data-load-s3.html)

### GitHub Actions
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [AWS Actions](https://github.com/aws-actions)