#!/usr/bin/env python3

"""
Setup Validation Script
=======================

This script validates that all required configurations are in place
for the CI/CD pipeline deployment.
"""

import os
import sys
import boto3
import json
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv

load_dotenv()


def check_file_exists(file_path: str) -> bool:
    """Check if a file exists"""
    return Path(file_path).exists()

def check_env_variables() -> Dict[str, Any]:
    """Check if required environment variables are set"""
    print("🔍 Checking environment variables...")
    
    required_vars = {
        'AWS': [
            'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY',
            'S3_BUCKET_NAME'
        ],
        'Snowflake': [
            'SNOWFLAKE_USER',
            'SNOWFLAKE_PASSWORD',
            'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_WAREHOUSE',
            'SNOWFLAKE_DATABASE',
            'SNOWFLAKE_SCHEMA'
        ]
    }
    
    results = {}
    
    for category, vars_list in required_vars.items():
        results[category] = {}
        for var in vars_list:
            value = os.getenv(var)
            results[category][var] = {
                'set': value is not None,
                'value': '***' if value and 'PASSWORD' in var or 'KEY' in var else value
            }
    
    return results

def check_aws_connectivity() -> Dict[str, Any]:
    """Check AWS connectivity and permissions"""
    print("☁️ Checking AWS connectivity...")
    
    try:
        # Test S3 access
        s3_client = boto3.client('s3')
        bucket_name = os.getenv('S3_BUCKET_NAME')
        
        if not bucket_name:
            return {'error': 'S3_BUCKET_NAME environment variable not set'}
        
        # Check if bucket exists and is accessible
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            bucket_accessible = True
        except Exception as e:
            bucket_accessible = False
            bucket_error = str(e)
        
        # Test Glue access
        try:
            glue_client = boto3.client('glue')
            glue_client.get_databases()
            glue_accessible = True
        except Exception as e:
            glue_accessible = False
            glue_error = str(e)
        
        # Test IAM access
        try:
            iam_client = boto3.client('iam')
            iam_client.get_user()
            iam_accessible = True
        except Exception as e:
            iam_accessible = False
            iam_error = str(e)
        
        return {
            's3': {
                'accessible': bucket_accessible,
                'bucket': bucket_name,
                'error': bucket_error if not bucket_accessible else None
            },
            'glue': {
                'accessible': glue_accessible,
                'error': glue_error if not glue_accessible else None
            },
            'iam': {
                'accessible': iam_accessible,
                'error': iam_error if not iam_accessible else None
            }
        }
        
    except Exception as e:
        return {'error': f'AWS configuration error: {str(e)}'}

def check_snowflake_connectivity() -> Dict[str, Any]:
    """Check Snowflake connectivity"""
    print("🏔️ Checking Snowflake connectivity...")
    
    try:
        import snowflake.connector
        
        conn_params = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA')
        }
        
        # Check if all parameters are set
        missing_params = [key for key, value in conn_params.items() if not value]
        if missing_params:
            return {'error': f'Missing Snowflake parameters: {missing_params}'}
        
        # Test connection
        try:
            conn = snowflake.connector.connect(**conn_params)
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            return {
                'accessible': True,
                'version': version,
                'database': conn_params['database'],
                'schema': conn_params['schema']
            }
            
        except Exception as e:
            return {
                'accessible': False,
                'error': str(e)
            }
            
    except ImportError:
        return {'error': 'snowflake-connector-python not installed'}

def check_required_files() -> Dict[str, bool]:
    """Check if all required files exist"""
    print("📁 Checking required files...")
    
    required_files = [
        # Glue job scripts
        'glue_jobs/01_raw_data_transformation.py',
        'glue_jobs/02_analytics_aggregation.py',
        'glue_jobs/03_time_series_analysis.py',
        
        # Snowflake scripts
        'snowflake/01_setup_stages.sql',
        'snowflake/02_create_file_formats.sql',
        'snowflake/03_create_external_tables.sql',
        'snowflake/04_sample_queries.sql',
        
        # Configuration files
        'snowflake_connector.py',
        '.github/workflows/deploy-pipeline.yml',
        
        # Documentation
        'docs/CICD_SETUP.md'
    ]
    
    results = {}
    for file_path in required_files:
        results[file_path] = check_file_exists(file_path)
    
    return results

def generate_github_secrets_template() -> str:
    """Generate a template for GitHub secrets"""
    template = """
# GitHub Secrets Configuration Template
# ====================================
# Copy these to your GitHub repository secrets

# AWS Credentials
AWS_ACCESS_KEY_ID=your_aws_access_key_here
AWS_SECRET_ACCESS_KEY=your_aws_secret_key_here
S3_BUCKET_NAME=your-s3-bucket-name

# Snowflake Credentials  
SNOWFLAKE_USER=INTERNPROJECT
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_ACCOUNT=your_account-INTERNPROJECT
SNOWFLAKE_WAREHOUSE=INT_WH
SNOWFLAKE_DATABASE=ECOMMERCE_DB
SNOWFLAKE_SCHEMA=STAGING

# Instructions:
# 1. Go to your GitHub repository
# 2. Navigate to Settings → Secrets and variables → Actions
# 3. Click "New repository secret" 
# 4. Add each secret above with the actual values
"""
    return template

def main():
    """Main validation function"""
    print("🚀 Data Pipeline Setup Validation")
    print("=" * 50)
    
    all_checks_passed = True
    
    # Check environment variables
    env_results = check_env_variables()
    print("\n📋 Environment Variables:")
    for category, vars_dict in env_results.items():
        print(f"\n  {category}:")
        for var, info in vars_dict.items():
            status = "✅" if info['set'] else "❌"
            print(f"    {status} {var}: {info['value'] if info['set'] else 'NOT SET'}")
            if not info['set']:
                all_checks_passed = False
    
    # Check required files
    print("\n📁 Required Files:")
    file_results = check_required_files()
    for file_path, exists in file_results.items():
        status = "✅" if exists else "❌"
        print(f"  {status} {file_path}")
        if not exists:
            all_checks_passed = False
    
    # Check AWS connectivity
    print("\n☁️ AWS Connectivity:")
    aws_results = check_aws_connectivity()
    if 'error' in aws_results:
        print(f"  ❌ {aws_results['error']}")
        all_checks_passed = False
    else:
        for service, info in aws_results.items():
            if isinstance(info, dict) and 'accessible' in info:
                status = "✅" if info['accessible'] else "❌"
                print(f"  {status} {service.upper()}: {'Accessible' if info['accessible'] else info.get('error', 'Error')}")
                if not info['accessible']:
                    all_checks_passed = False
    
    # Check Snowflake connectivity
    print("\n🏔️ Snowflake Connectivity:")
    snowflake_results = check_snowflake_connectivity()
    if 'error' in snowflake_results:
        print(f"  ❌ {snowflake_results['error']}")
        all_checks_passed = False
    elif snowflake_results.get('accessible'):
        print(f"  ✅ Connected to Snowflake {snowflake_results['version']}")
        print(f"  ✅ Database: {snowflake_results['database']}")
        print(f"  ✅ Schema: {snowflake_results['schema']}")
    else:
        print(f"  ❌ Connection failed: {snowflake_results.get('error', 'Unknown error')}")
        all_checks_passed = False
    
    # Summary
    print("\n" + "=" * 50)
    if all_checks_passed:
        print("🎉 ALL CHECKS PASSED!")
        print("✅ Your setup is ready for CI/CD deployment")
        print("\n🚀 Next steps:")
        print("1. Configure GitHub secrets (see template below)")
        print("2. Push changes to main branch")
        print("3. Monitor GitHub Actions workflow")
        print("4. Run Glue jobs in AWS Console")
    else:
        print("❌ SOME CHECKS FAILED")
        print("🔧 Please fix the issues above before deployment")
        
    # Generate GitHub secrets template
    print("\n📝 GitHub Secrets Template:")
    print(generate_github_secrets_template())
    
    return all_checks_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 