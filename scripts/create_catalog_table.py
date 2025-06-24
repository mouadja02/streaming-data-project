#!/usr/bin/env python3

"""
Create Glue Catalog Table for Data Pipeline
==========================================

This script creates the Iceberg table in AWS Glue Data Catalog
that the data-pipeline-raw-transformation job will write to.
"""

import boto3
import json
from dotenv import load_dotenv

load_dotenv()

def create_catalog_table():
    """Create the users_cleaned table in Glue Data Catalog"""
    
    glue_client = boto3.client('glue')
    
    # Table definition
    table_input = {
        'Name': 'users_cleaned',
        'Description': 'Cleaned and enriched user data from raw transformation job',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'id', 'Type': 'string', 'Comment': 'User unique identifier'},
                {'Name': 'first_name', 'Type': 'string', 'Comment': 'User first name'},
                {'Name': 'last_name', 'Type': 'string', 'Comment': 'User last name'},
                {'Name': 'gender', 'Type': 'string', 'Comment': 'User gender'},
                {'Name': 'address', 'Type': 'string', 'Comment': 'Full address'},
                {'Name': 'post_code', 'Type': 'string', 'Comment': 'Postal code'},
                {'Name': 'email', 'Type': 'string', 'Comment': 'Email address'},
                {'Name': 'username', 'Type': 'string', 'Comment': 'Username'},
                {'Name': 'dob', 'Type': 'date', 'Comment': 'Date of birth'},
                {'Name': 'registered_date', 'Type': 'date', 'Comment': 'Registration date'},
                {'Name': 'phone', 'Type': 'string', 'Comment': 'Phone number'},
                {'Name': 'picture', 'Type': 'string', 'Comment': 'Profile picture URL'},
                {'Name': 'age', 'Type': 'int', 'Comment': 'Calculated age'},
                {'Name': 'age_group', 'Type': 'string', 'Comment': 'Age category (Young Adult, Adult, etc.)'},
                {'Name': 'generation', 'Type': 'string', 'Comment': 'Generation category (Gen Z, Millennial, etc.)'},
                {'Name': 'email_domain', 'Type': 'string', 'Comment': 'Email domain extracted'},
                {'Name': 'email_provider_type', 'Type': 'string', 'Comment': 'Email provider type (Personal, Business, etc.)'},
                {'Name': 'phone_cleaned', 'Type': 'string', 'Comment': 'Cleaned and standardized phone number'},
                {'Name': 'data_quality_score', 'Type': 'double', 'Comment': 'Data quality score (0.0 to 1.0)'},
                {'Name': 'processing_timestamp', 'Type': 'timestamp', 'Comment': 'When record was processed'}
            ],
            'Location': 's3://my-amazing-app/iceberg-warehouse/users_cleaned/',
            'InputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergInputFormat',
            'OutputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.iceberg.mr.mapreduce.IcebergSerDe'
            },
            'Parameters': {
                'table_type': 'ICEBERG',
                'metadata_location': 's3://my-amazing-app/iceberg-warehouse/users_cleaned/metadata/',
                'write.format.default': 'parquet',
                'write.parquet.compression-codec': 'snappy'
            }
        },
        'PartitionKeys': [
            {'Name': 'year', 'Type': 'string', 'Comment': 'Partition by year for performance'},
            {'Name': 'month', 'Type': 'string', 'Comment': 'Partition by month for performance'}
        ],
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'iceberg',
            'table_type': 'ICEBERG',
            'metadata_location': 's3://my-amazing-app/iceberg-warehouse/users_cleaned/metadata/'
        }
    }
    
    try:
        # Create database first (ignore if exists)
        try:
            glue_client.create_database(
                DatabaseInput={
                    'Name': 'data_pipeline_db',
                    'Description': 'Database for data pipeline Iceberg tables'
                }
            )
            print("✅ Database 'data_pipeline_db' created successfully")
        except glue_client.exceptions.AlreadyExistsException:
            print("✅ Database 'data_pipeline_db' already exists")
        
        # Create table
        response = glue_client.create_table(
            DatabaseName='data_pipeline_db',
            TableInput=table_input
        )
        
        print("✅ Table 'users_cleaned' created successfully!")
        print(f"📍 Location: s3://my-amazing-app/iceberg-warehouse/users_cleaned/")
        print(f"🗄️ Database: data_pipeline_db")
        print(f"📊 Columns: {len(table_input['StorageDescriptor']['Columns'])}")
        print(f"🔧 Table Type: Apache Iceberg")
        
        return True
        
    except glue_client.exceptions.AlreadyExistsException:
        print("⚠️ Table 'users_cleaned' already exists")
        
        # Update table instead
        try:
            glue_client.update_table(
                DatabaseName='data_pipeline_db',
                TableInput=table_input
            )
            print("✅ Table 'users_cleaned' updated successfully!")
            return True
        except Exception as e:
            print(f"❌ Error updating table: {str(e)}")
            return False
            
    except Exception as e:
        print(f"❌ Error creating table: {str(e)}")
        return False

def verify_table():
    """Verify the table was created correctly"""
    
    glue_client = boto3.client('glue')
    
    try:
        response = glue_client.get_table(
            DatabaseName='data_pipeline_db',
            Name='users_cleaned'
        )
        
        table = response['Table']
        print("\n🔍 Table Verification:")
        print(f"✅ Name: {table['Name']}")
        print(f"✅ Database: {table['DatabaseName']}")
        print(f"✅ Type: {table.get('Parameters', {}).get('table_type', 'Unknown')}")
        print(f"✅ Location: {table['StorageDescriptor']['Location']}")
        print(f"✅ Columns: {len(table['StorageDescriptor']['Columns'])}")
        print(f"✅ Partitions: {len(table.get('PartitionKeys', []))}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error verifying table: {str(e)}")
        return False

def main():
    """Main function"""
    print("🚀 Creating Glue Catalog Table for Data Pipeline")
    print("=" * 60)
    
    # Create table
    if create_catalog_table():
        print("\n" + "=" * 60)
        
        # Verify table
        if verify_table():
            print("\n🎉 SUCCESS! Your catalog table is ready!")
            print("\n📋 Next Steps:")
            print("1. Go to AWS Glue Console → Data Catalog → Tables")
            print("2. Find 'users_cleaned' table in 'data_pipeline_db' database")
            print("3. Run your Glue job 'data-pipeline-raw-transformation'")
            print("4. The job will write data to this Iceberg table")
            print("\n🔗 Useful Links:")
            print("• Glue Console: https://console.aws.amazon.com/glue/")
            print("• Athena Console: https://console.aws.amazon.com/athena/")
            print("• S3 Location: s3://my-amazing-app/iceberg-warehouse/users_cleaned/")
        else:
            print("\n⚠️ Table created but verification failed")
    else:
        print("\n❌ Failed to create catalog table")

if __name__ == "__main__":
    main() 