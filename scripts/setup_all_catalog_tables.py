#!/usr/bin/env python3

"""
Setup All Glue Catalog Tables
=============================

This script creates all Iceberg tables in AWS Glue Data Catalog
for the three data pipeline jobs using schemas from catalog_table_schemas.json.
"""

import boto3
import json
import sys
import os
from dotenv import load_dotenv

load_dotenv()

def get_glue_client():
    """Get Glue client"""
    return boto3.client('glue')

def load_catalog_schemas():
    """Load table schemas from catalog_table_schemas.json"""
    schema_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'docs', 'catalog_table_schemas.json')
    
    try:
        with open(schema_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Schema file not found: {schema_file}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing schema file: {str(e)}")
        sys.exit(1)

def convert_column_type(json_type):
    """Convert JSON schema type to AWS Glue type"""
    type_mapping = {
        'string': 'string',
        'int': 'int',
        'bigint': 'bigint',
        'double': 'double',
        'boolean': 'boolean',
        'date': 'date',
        'timestamp': 'timestamp',
        'array<string>': 'array<string>',
        'map<string,bigint>': 'map<string,bigint>'
    }
    return type_mapping.get(json_type, json_type)

def create_table_from_schema(table_name, table_schema):
    """Create a table using schema from JSON"""
    glue_client = get_glue_client()
    
    # Convert columns from JSON schema to Glue format
    columns = []
    partition_keys = []
    
    for col in table_schema['columns']:
        glue_column = {
            'Name': col['name'],
            'Type': convert_column_type(col['type']),
            'Comment': col['comment']
        }
        
        # Check if this column is a partition
        if col['name'] in table_schema.get('partitions', []):
            partition_keys.append(glue_column)
        else:
            columns.append(glue_column)
    
    table_input = {
        'Name': table_name,
        'Description': table_schema['description'],
        'StorageDescriptor': {
            'Columns': columns,
            'Location': table_schema['location'],
            'InputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergInputFormat',
            'OutputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.iceberg.mr.mapreduce.IcebergSerDe'
            },
            'Parameters': {
                'table_type': 'ICEBERG',
                'metadata_location': f"{table_schema['location']}metadata/"
            }
        },
        'PartitionKeys': partition_keys,
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'iceberg',
            'table_type': 'ICEBERG'
        }
    }
    
    return create_table_helper(glue_client, table_name, table_input)

def create_database():
    """Create the pipeline database"""
    glue_client = get_glue_client()
    
    try:
        glue_client.create_database(
            DatabaseInput={
                'Name': 'data_pipeline_db',
                'Description': 'Database for data pipeline Iceberg tables'
            }
        )
        print("✅ Database 'data_pipeline_db' created successfully")
        return True
    except glue_client.exceptions.AlreadyExistsException:
        print("✅ Database 'data_pipeline_db' already exists")
        return True
    except Exception as e:
        print(f"❌ Error creating database: {str(e)}")
        return False

def create_table_helper(glue_client, table_name, table_input):
    """Helper function to create or update a table"""
    try:
        glue_client.create_table(
            DatabaseName='data_pipeline_db',
            TableInput=table_input
        )
        print(f"✅ Table '{table_name}' created successfully!")
        return True
        
    except glue_client.exceptions.AlreadyExistsException:
        print(f"⚠️ Table '{table_name}' already exists - updating...")
        try:
            glue_client.update_table(
                DatabaseName='data_pipeline_db',
                TableInput=table_input
            )
            print(f"✅ Table '{table_name}' updated successfully!")
            return True
        except Exception as e:
            print(f"❌ Error updating table '{table_name}': {str(e)}")
            return False
            
    except Exception as e:
        print(f"❌ Error creating table '{table_name}': {str(e)}")
        return False

def main():
    """Main function to create all catalog tables"""
    print("Setting Up All Glue Catalog Tables")
    print("=" * 60)
    
    # Load schemas from JSON file
    print("📋 Loading table schemas from catalog_table_schemas.json...")
    catalog_schemas = load_catalog_schemas()
    
    # Create database
    if not create_database():
        print("❌ Failed to create database. Exiting.")
        sys.exit(1)
    
    print(f"\n🔄 Creating {len(catalog_schemas['tables'])} tables...")
    
    # Track success
    tables_created = []
    tables_failed = []
    
    # Group tables by job for organized output
    job_tables = {
        'data-pipeline-raw-transformation': [],
        'data-pipeline-analytics-aggregation': [],
        'data-pipeline-time-series-analysis': []
    }
    
    # Categorize tables by job
    for table_name, table_schema in catalog_schemas['tables'].items():
        job = table_schema.get('job', 'unknown')
        if job in job_tables:
            job_tables[job].append((table_name, table_schema))
        else:
            job_tables['data-pipeline-analytics-aggregation'].append((table_name, table_schema))
    
    # Create tables by job
    for job_name, tables in job_tables.items():
        if not tables:
            continue
            
        print(f"\n🔄 {job_name.replace('-', ' ').title()} Tables:")
        
        for table_name, table_schema in tables:
            print(f"   Creating table: {table_name}")
            
            if create_table_from_schema(table_name, table_schema):
                tables_created.append(table_name)
            else:
                tables_failed.append(table_name)
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 SETUP SUMMARY")
    print("=" * 60)
    print(f"✅ Tables created successfully: {len(tables_created)}")
    
    if tables_created:
        for table in tables_created:
            print(f"   • {table}")
    
    if tables_failed:
        print(f"\n❌ Tables failed: {len(tables_failed)}")
        for table in tables_failed:
            print(f"   • {table}")
        sys.exit(1)
    else:
        print(f"\n🎉 All {len(tables_created)} tables created successfully!")
        print(f"📍 Database: {catalog_schemas['database']}")
        print(f"📍 Warehouse Location: {catalog_schemas['summary']['iceberg_warehouse_location']}")

if __name__ == "__main__":
    main() 