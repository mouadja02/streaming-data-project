#!/usr/bin/env python3

"""
Check Iceberg Metadata Script
=============================
This script checks Iceberg table metadata and structure.
"""

import boto3
import json
import sys
from dotenv import load_dotenv

load_dotenv()

def get_glue_client():
    return boto3.client('glue')

def get_s3_client():
    return boto3.client('s3')

def check_iceberg_table_metadata(database_name: str, table_name: str):
    glue_client = get_glue_client()
    
    try:
        response = glue_client.get_table(
            DatabaseName=database_name,
            Name=table_name
        )
        
        table = response['Table']
        
        print(f"✅ Table: {table_name}")
        print(f"   Description: {table.get('Description', 'N/A')}")
        print(f"   Location: {table['StorageDescriptor']['Location']}")
        print(f"   Input Format: {table['StorageDescriptor']['InputFormat']}")
        print(f"   Output Format: {table['StorageDescriptor']['OutputFormat']}")
        print(f"   Serde: {table['StorageDescriptor']['SerdeInfo']['SerializationLibrary']}")
        
        if table.get('PartitionKeys'):
            print(f"   Partitions: {[p['Name'] for p in table['PartitionKeys']]}")
        
        print(f"   Columns ({len(table['StorageDescriptor']['Columns'])}):")
        for col in table['StorageDescriptor']['Columns'][:5]:
            print(f"     - {col['Name']} ({col['Type']})")
        
        if len(table['StorageDescriptor']['Columns']) > 5:
            print(f"     ... and {len(table['StorageDescriptor']['Columns']) - 5} more")
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking table {table_name}: {str(e)}")
        return False

def check_s3_location(s3_path: str):
    s3_client = get_s3_client()
    
    try:
        if s3_path.startswith('s3://'):
            bucket_and_key = s3_path[5:].split('/', 1)
            bucket = bucket_and_key[0]
            prefix = bucket_and_key[1] if len(bucket_and_key) > 1 else ''
        else:
            print(f"❌ Invalid S3 path format: {s3_path}")
            return False
        
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=10
        )
        
        if 'Contents' in response:
            print(f"✅ S3 Location: {s3_path}")
            print(f"   Objects found: {len(response['Contents'])}")
            for obj in response['Contents'][:3]:
                print(f"     - {obj['Key']} ({obj['Size']} bytes)")
            
            if len(response['Contents']) > 3:
                print(f"     ... and {len(response['Contents']) - 3} more")
            
            return True
        else:
            print(f"❌ No objects found at: {s3_path}")
            return False
            
    except Exception as e:
        print(f"❌ Error checking S3 location {s3_path}: {str(e)}")
        return False

def main():
    print("Iceberg Metadata Checker")
    print("=" * 50)
    
    database_name = 'data_pipeline_db'
    
    tables_to_check = [
        'users_transformed',
        'data_quality_summary',
        'dim_user_demographics',
        'fact_geographic_analysis',
        'fact_age_generation_analysis',
        'fact_email_provider_analysis',
        'fact_email_domain_analysis',
        'fact_data_quality_metrics',
        'fact_quality_by_segment'
    ]
    
    print(f"Checking {len(tables_to_check)} tables in database: {database_name}")
    
    successful_checks = 0
    
    for table_name in tables_to_check:
        print(f"\n🔍 Checking table: {table_name}")
        
        if check_iceberg_table_metadata(database_name, table_name):
            successful_checks += 1
    
    print(f"\n{'='*50}")
    print(f"Summary: {successful_checks}/{len(tables_to_check)} tables checked successfully")
    
    if successful_checks == len(tables_to_check):
        print("✅ All tables are properly configured!")
    else:
        print("❌ Some tables have issues. Check the logs above.")

if __name__ == "__main__":
    main() 