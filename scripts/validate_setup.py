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
    return Path(file_path).exists()

def check_env_variables() -> Dict[str, Any]:
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
                'value': '***' if value and ('PASSWORD' in var or 'KEY' in var) else value
            }
    
    return results

def check_aws_connectivity() -> Dict[str, Any]:
    print("☁️ Checking AWS connectivity...")
    
    try:
        s3_client = boto3.client('s3')
        bucket_name = os.getenv('S3_BUCKET_NAME')
        
        if not bucket_name:
            return {'error': 'S3_BUCKET_NAME environment variable not set'}
        
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            bucket_accessible = True
            bucket_error = None
        except Exception as e:
            bucket_accessible = False
            bucket_error = str(e)
        
        try:
            glue_client = boto3.client('glue')
            glue_client.get_databases()
            glue_accessible = True
            glue_error = None
        except Exception as e:
            glue_accessible = False
            glue_error = str(e)
        
        try:
            iam_client = boto3.client('iam')
            iam_client.get_user()
            iam_accessible = True
            iam_error = None
        except Exception as e:
            iam_accessible = False
            iam_error = str(e)
        
        return {
            's3': {
                'accessible': bucket_accessible,
                'bucket': bucket_name,
                'error': bucket_error
            },
            'glue': {
                'accessible': glue_accessible,
                'error': glue_error
            },
            'iam': {
                'accessible': iam_accessible,
                'error': iam_error
            }
        }
        
    except Exception as e:
        return {'error': f'AWS configuration error: {str(e)}'}

def check_snowflake_connectivity() -> Dict[str, Any]:
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
        
        missing_params = [key for key, value in conn_params.items() if not value]
        if missing_params:
            return {'error': f'Missing Snowflake parameters: {missing_params}'}
        
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
    print("📁 Checking required files...")
    
    required_files = [
        'glue_jobs/01_raw_data_transformation.py',
        'glue_jobs/02_analytics_aggregation.py',
        'glue_jobs/03_time_series_analysis.py',
        'snowflake/00_setup_stages.sql',
        'snowflake/01_create_file_formats.sql',
        'snowflake/02_bronze_layer.sql',
        'snowflake/03_bronze_checks.sql',
        'snowflake/04_silver_layer.sql',
        'snowflake/06_create_gold_views.sql',
        'snowflake/06_final_checks.sql',
        'snowflake_connector.py',
        '.github/workflows/deploy-pipeline.yml',
        'docs/CICD_SETUP.md'
    ]
    
    results = {}
    for file_path in required_files:
        results[file_path] = check_file_exists(file_path)
    
    return results

def generate_github_secrets_template() -> str:
    template = """
# GitHub Secrets Configuration Template
# ====================================

# AWS Credentials
AWS_ACCESS_KEY_ID=your_aws_access_key_here
AWS_SECRET_ACCESS_KEY=your_aws_secret_key_here
S3_BUCKET_NAME=your_s3_bucket_name
AWS_REGION=us-east-1

# Snowflake Credentials
SNOWFLAKE_USER=your_snowflake_user
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_ACCOUNT=your_snowflake_account
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
"""
    return template.strip()

def print_validation_results(results: Dict[str, Any]):
    print("\n" + "="*60)
    print("VALIDATION RESULTS")
    print("="*60)
    
    # Environment Variables
    print("\n📋 Environment Variables:")
    env_results = results.get('env_variables', {})
    for category, vars_dict in env_results.items():
        print(f"\n  {category}:")
        for var, info in vars_dict.items():
            status = "✅" if info['set'] else "❌"
            value = info['value'] if info['set'] else "Not set"
            print(f"    {status} {var}: {value}")
    
    # AWS Connectivity
    print("\n☁️ AWS Connectivity:")
    aws_results = results.get('aws_connectivity', {})
    if 'error' in aws_results:
        print(f"  ❌ {aws_results['error']}")
    else:
        for service, info in aws_results.items():
            status = "✅" if info['accessible'] else "❌"
            print(f"  {status} {service.upper()}: {'Accessible' if info['accessible'] else info.get('error', 'Not accessible')}")
    
    # Snowflake Connectivity
    print("\n🏔️ Snowflake Connectivity:")
    sf_results = results.get('snowflake_connectivity', {})
    if 'error' in sf_results:
        print(f"  ❌ {sf_results['error']}")
    else:
        status = "✅" if sf_results.get('accessible') else "❌"
        if sf_results.get('accessible'):
            print(f"  {status} Connected to {sf_results['database']}.{sf_results['schema']}")
            print(f"      Version: {sf_results['version']}")
        else:
            print(f"  {status} {sf_results.get('error', 'Connection failed')}")
    
    # Required Files
    print("\n📁 Required Files:")
    file_results = results.get('required_files', {})
    for file_path, exists in file_results.items():
        status = "✅" if exists else "❌"
        print(f"  {status} {file_path}")

def main():
    print("Setup Validation")
    print("="*50)
    print("Validating CI/CD pipeline configuration...")
    
    results = {}
    
    results['env_variables'] = check_env_variables()
    results['aws_connectivity'] = check_aws_connectivity()
    results['snowflake_connectivity'] = check_snowflake_connectivity()
    results['required_files'] = check_required_files()
    
    print_validation_results(results)
    
    # Summary
    env_ok = all(
        all(var_info['set'] for var_info in category.values())
        for category in results['env_variables'].values()
    )
    
    aws_ok = (
        'error' not in results['aws_connectivity'] and
        all(service['accessible'] for service in results['aws_connectivity'].values())
    )
    
    sf_ok = results['snowflake_connectivity'].get('accessible', False)
    
    files_ok = all(results['required_files'].values())
    
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Environment Variables: {'✅' if env_ok else '❌'}")
    print(f"AWS Connectivity: {'✅' if aws_ok else '❌'}")
    print(f"Snowflake Connectivity: {'✅' if sf_ok else '❌'}")
    print(f"Required Files: {'✅' if files_ok else '❌'}")
    
    overall_status = env_ok and aws_ok and sf_ok and files_ok
    print(f"\nOverall Status: {'✅ READY FOR DEPLOYMENT' if overall_status else '❌ ISSUES FOUND'}")
    
    if not overall_status:
        print("\nGitHub Secrets Template:")
        print(generate_github_secrets_template())
        print("\nPlease fix the issues above before deploying.")
        sys.exit(1)
    else:
        print("\n🎉 All validations passed! You're ready to deploy.")

if __name__ == "__main__":
    main() 