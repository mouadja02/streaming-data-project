#!/usr/bin/env python3

"""
Check Iceberg Metadata in S3
============================

This script checks if Iceberg metadata files exist in S3 for the tables.
"""

import boto3
from botocore.exceptions import ClientError
import sys

def check_s3_path(s3_client, bucket, prefix):
    """Check if S3 path exists and list contents"""
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=10
        )
        
        if 'Contents' in response:
            print(f"✅ Found {len(response['Contents'])} objects at s3://{bucket}/{prefix}")
            for obj in response['Contents'][:5]:  # Show first 5 objects
                print(f"   📄 {obj['Key']} ({obj['Size']} bytes)")
            if len(response['Contents']) > 5:
                print(f"   ... and {len(response['Contents']) - 5} more objects")
            return True
        else:
            print(f"❌ No objects found at s3://{bucket}/{prefix}")
            return False
            
    except ClientError as e:
        print(f"❌ Error accessing s3://{bucket}/{prefix}: {e}")
        return False

def main():
    """Check Iceberg metadata for key tables"""
    print("🔍 Checking Iceberg Metadata in S3")
    print("=" * 50)
    
    s3_client = boto3.client('s3')
    bucket = 'my-amazing-app'
    
    # Tables to check
    tables_to_check = [
        'users_transformed_parquet',
        'users_transformed', 
        'data_quality_summary_parquet',
        'dim_user_demographics_parquet',
        'fact_geographic_analysis_parquet'
    ]
    
    print(f"\n📦 Checking bucket: {bucket}")
    print(f"🏠 Base path: iceberg-warehouse/")
    
    all_good = True
    
    for table in tables_to_check:
        print(f"\n🔍 Checking table: {table}")
        
        # Check data files
        data_path = f"iceberg-warehouse/{table}/"
        has_data = check_s3_path(s3_client, bucket, data_path)
        
        # Check metadata specifically
        metadata_path = f"iceberg-warehouse/{table}/metadata/"
        has_metadata = check_s3_path(s3_client, bucket, metadata_path)
        
        if not has_data and not has_metadata:
            all_good = False
            print(f"⚠️ Table {table} has no data or metadata files")
        elif has_data and not has_metadata:
            print(f"⚠️ Table {table} has data but no metadata directory")
            all_good = False
        elif has_metadata:
            print(f"✅ Table {table} appears to have proper Iceberg structure")
    
    print("\n" + "=" * 50)
    if all_good:
        print("🎉 All tables have proper Iceberg metadata!")
    else:
        print("⚠️ Some tables are missing metadata. You need to run Glue jobs first.")
        print("\n💡 Solution:")
        print("1. Run your Glue job: data-pipeline-raw-transformation")
        print("2. This will create the proper Iceberg metadata files")
        print("3. Then Snowflake can read the table properly")
        
    print(f"\n🔗 S3 Console: https://s3.console.aws.amazon.com/s3/buckets/{bucket}/iceberg-warehouse/")

if __name__ == "__main__":
    main() 