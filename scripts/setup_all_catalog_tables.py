#!/usr/bin/env python3

"""
Setup All Glue Catalog Tables
=============================

This script creates all Iceberg tables in AWS Glue Data Catalog
for the three data pipeline jobs.
"""

import boto3
import json
import sys
from dotenv import load_dotenv

load_dotenv()

def get_glue_client():
    """Get Glue client"""
    return boto3.client('glue')

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

def create_users_transformed_table():
    """Table for Job 1: Raw Data Transformation - users_transformed"""
    glue_client = get_glue_client()
    
    table_input = {
        'Name': 'users_transformed',
        'Description': 'Transformed and enriched user data from raw transformation job',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'id', 'Type': 'string', 'Comment': 'User unique identifier'},
                {'Name': 'first_name', 'Type': 'string', 'Comment': 'User first name (cleaned)'},
                {'Name': 'last_name', 'Type': 'string', 'Comment': 'User last name (cleaned)'},
                {'Name': 'full_name', 'Type': 'string', 'Comment': 'Full name concatenated'},
                {'Name': 'gender', 'Type': 'string', 'Comment': 'User gender'},
                {'Name': 'email', 'Type': 'string', 'Comment': 'Email address'},
                {'Name': 'email_domain', 'Type': 'string', 'Comment': 'Email domain extracted'},
                {'Name': 'email_provider_type', 'Type': 'string', 'Comment': 'Email provider type'},
                {'Name': 'email_valid', 'Type': 'boolean', 'Comment': 'Email validation flag'},
                {'Name': 'username', 'Type': 'string', 'Comment': 'Username'},
                {'Name': 'phone', 'Type': 'string', 'Comment': 'Phone number (formatted)'},
                {'Name': 'phone_valid', 'Type': 'boolean', 'Comment': 'Phone validation flag'},
                {'Name': 'address', 'Type': 'string', 'Comment': 'Full address'},
                {'Name': 'street_address', 'Type': 'string', 'Comment': 'Street address parsed'},
                {'Name': 'city', 'Type': 'string', 'Comment': 'City name'},
                {'Name': 'country', 'Type': 'string', 'Comment': 'Country name'},
                {'Name': 'post_code', 'Type': 'string', 'Comment': 'Postal code'},
                {'Name': 'birth_date', 'Type': 'date', 'Comment': 'Date of birth'},
                {'Name': 'age_years', 'Type': 'int', 'Comment': 'Calculated age in years'},
                {'Name': 'age_group', 'Type': 'string', 'Comment': 'Age category'},
                {'Name': 'generation', 'Type': 'string', 'Comment': 'Generation category'},
                {'Name': 'registration_date', 'Type': 'date', 'Comment': 'Registration date'},
                {'Name': 'account_age_days', 'Type': 'int', 'Comment': 'Account age in days'},
                {'Name': 'account_tenure_category', 'Type': 'string', 'Comment': 'Account tenure category'},
                {'Name': 'data_quality_score', 'Type': 'int', 'Comment': 'Data quality score'},
                {'Name': 'quality_issues', 'Type': 'array<string>', 'Comment': 'List of quality issues'},
                {'Name': 'picture', 'Type': 'string', 'Comment': 'Profile picture URL'},
                {'Name': 'processed_at', 'Type': 'timestamp', 'Comment': 'Processing timestamp'},
                {'Name': 'processing_date', 'Type': 'date', 'Comment': 'Processing date'},
                {'Name': 'data_source', 'Type': 'string', 'Comment': 'Data source identifier'},
                {'Name': 'glue_job_name', 'Type': 'string', 'Comment': 'Glue job name that processed this record'}
            ],
            'Location': 's3://my-amazing-app/iceberg-warehouse/users_transformed_parquet/',
            'InputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergInputFormat',
            'OutputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.iceberg.mr.mapreduce.IcebergSerDe'
            },
            'Parameters': {
                'table_type': 'ICEBERG',
                'metadata_location': 's3://my-amazing-app/iceberg-warehouse/users_transformed_parquet/metadata/'
            }
        },
        'PartitionKeys': [
            {'Name': 'processing_date', 'Type': 'date', 'Comment': 'Processing date partition'}
        ],
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'iceberg',
            'table_type': 'ICEBERG'
        }
    }
    
    return create_table_helper(glue_client, 'users_transformed', table_input)

def create_data_quality_summary_table():
    """Table for Job 1: Data Quality Summary"""
    glue_client = get_glue_client()
    
    table_input = {
        'Name': 'data_quality_summary',
        'Description': 'Data quality metrics and summary statistics',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'total_records', 'Type': 'bigint', 'Comment': 'Total number of records processed'},
                {'Name': 'high_quality_records', 'Type': 'bigint', 'Comment': 'Records with quality score >= 80'},
                {'Name': 'medium_quality_records', 'Type': 'bigint', 'Comment': 'Records with quality score 50-79'},
                {'Name': 'low_quality_records', 'Type': 'bigint', 'Comment': 'Records with quality score < 50'},
                {'Name': 'avg_quality_score', 'Type': 'double', 'Comment': 'Average quality score'},
                {'Name': 'valid_emails', 'Type': 'bigint', 'Comment': 'Number of valid email addresses'},
                {'Name': 'valid_phones', 'Type': 'bigint', 'Comment': 'Number of valid phone numbers'},
                {'Name': 'unique_email_domains', 'Type': 'bigint', 'Comment': 'Number of unique email domains'},
                {'Name': 'unique_countries', 'Type': 'bigint', 'Comment': 'Number of unique countries'},
                {'Name': 'quality_check_timestamp', 'Type': 'timestamp', 'Comment': 'Quality check timestamp'},
                {'Name': 'processing_date', 'Type': 'date', 'Comment': 'Processing date'},
                {'Name': 'glue_job_name', 'Type': 'string', 'Comment': 'Glue job name'}
            ],
            'Location': 's3://my-amazing-app/iceberg-warehouse/data_quality_summary_parquet/',
            'InputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergInputFormat',
            'OutputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.iceberg.mr.mapreduce.IcebergSerDe'
            },
            'Parameters': {
                'table_type': 'ICEBERG',
                'metadata_location': 's3://my-amazing-app/iceberg-warehouse/data_quality_summary_parquet/metadata/'
            }
        },
        'PartitionKeys': [
            {'Name': 'processing_date', 'Type': 'date', 'Comment': 'Processing date partition'}
        ],
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'iceberg',
            'table_type': 'ICEBERG'
        }
    }
    
    return create_table_helper(glue_client, 'data_quality_summary', table_input)

def create_users_cleaned_table():
    """Table for Job 1: Raw Data Transformation"""
    glue_client = get_glue_client()
    
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
                {'Name': 'age_group', 'Type': 'string', 'Comment': 'Age category'},
                {'Name': 'generation', 'Type': 'string', 'Comment': 'Generation category'},
                {'Name': 'email_domain', 'Type': 'string', 'Comment': 'Email domain extracted'},
                {'Name': 'email_provider_type', 'Type': 'string', 'Comment': 'Email provider type'},
                {'Name': 'phone_cleaned', 'Type': 'string', 'Comment': 'Cleaned phone number'},
                {'Name': 'data_quality_score', 'Type': 'double', 'Comment': 'Data quality score'},
                {'Name': 'processing_timestamp', 'Type': 'timestamp', 'Comment': 'Processing timestamp'}
            ],
            'Location': 's3://my-amazing-app/iceberg-warehouse/users_transformed/',
            'InputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergInputFormat',
            'OutputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.iceberg.mr.mapreduce.IcebergSerDe'
            },
            'Parameters': {
                'table_type': 'ICEBERG',
                'metadata_location': 's3://my-amazing-app/iceberg-warehouse/users_transformed/metadata/'
            }
        },
        'PartitionKeys': [
            {'Name': 'year', 'Type': 'string', 'Comment': 'Year partition'},
            {'Name': 'month', 'Type': 'string', 'Comment': 'Month partition'}
        ],
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'iceberg',
            'table_type': 'ICEBERG'
        }
    }
    
    return create_table_helper(glue_client, 'users_cleaned', table_input)

def create_user_demographics_table():
    """Table for Job 2: Analytics Aggregation - Demographics"""
    glue_client = get_glue_client()
    
    table_input = {
        'Name': 'user_demographics',
        'Description': 'User demographics dimension table with scoring and segmentation',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'demographic_key', 'Type': 'string', 'Comment': 'Unique demographic key'},
                {'Name': 'gender', 'Type': 'string', 'Comment': 'User gender'},
                {'Name': 'age_group', 'Type': 'string', 'Comment': 'Age group category'},
                {'Name': 'generation', 'Type': 'string', 'Comment': 'Generation category'},
                {'Name': 'user_count', 'Type': 'bigint', 'Comment': 'Number of users in this demographic'},
                {'Name': 'avg_age', 'Type': 'double', 'Comment': 'Average age in this demographic'},
                {'Name': 'avg_quality_score', 'Type': 'double', 'Comment': 'Average data quality score'},
                {'Name': 'engagement_score', 'Type': 'double', 'Comment': 'Calculated engagement score'},
                {'Name': 'segment', 'Type': 'string', 'Comment': 'User segment (High Value, Standard, etc.)'},
                {'Name': 'created_at', 'Type': 'timestamp', 'Comment': 'Record creation timestamp'},
                {'Name': 'updated_at', 'Type': 'timestamp', 'Comment': 'Last update timestamp'}
            ],
            'Location': 's3://my-amazing-app/iceberg-warehouse/dim_user_demographics_parquet/',
            'InputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergInputFormat',
            'OutputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.iceberg.mr.mapreduce.IcebergSerDe'
            },
            'Parameters': {
                'table_type': 'ICEBERG',
                'metadata_location': 's3://my-amazing-app/iceberg-warehouse/dim_user_demographics_parquet/metadata/'
            }
        },
        'PartitionKeys': [
            {'Name': 'segment', 'Type': 'string', 'Comment': 'Partition by user segment'}
        ],
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'iceberg',
            'table_type': 'ICEBERG'
        }
    }
    
    return create_table_helper(glue_client, 'user_demographics', table_input)

def create_geographic_analysis_table():
    """Table for Job 2: Analytics Aggregation - Geographic"""
    glue_client = get_glue_client()
    
    table_input = {
        'Name': 'geographic_analysis',
        'Description': 'Geographic analysis at country and city levels',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'location_key', 'Type': 'string', 'Comment': 'Unique location identifier'},
                {'Name': 'country', 'Type': 'string', 'Comment': 'Country name'},
                {'Name': 'state_province', 'Type': 'string', 'Comment': 'State or province'},
                {'Name': 'city', 'Type': 'string', 'Comment': 'City name'},
                {'Name': 'user_count', 'Type': 'bigint', 'Comment': 'Number of users in this location'},
                {'Name': 'gender_distribution', 'Type': 'map<string,bigint>', 'Comment': 'Gender distribution map'},
                {'Name': 'avg_age', 'Type': 'double', 'Comment': 'Average age in this location'},
                {'Name': 'dominant_age_group', 'Type': 'string', 'Comment': 'Most common age group'},
                {'Name': 'market_penetration_score', 'Type': 'double', 'Comment': 'Market penetration score'},
                {'Name': 'created_at', 'Type': 'timestamp', 'Comment': 'Record creation timestamp'},
                {'Name': 'updated_at', 'Type': 'timestamp', 'Comment': 'Last update timestamp'}
            ],
            'Location': 's3://my-amazing-app/iceberg-warehouse/fact_geographic_analysis_parquet/',
            'InputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergInputFormat',
            'OutputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.iceberg.mr.mapreduce.IcebergSerDe'
            },
            'Parameters': {
                'table_type': 'ICEBERG',
                'metadata_location': 's3://my-amazing-app/iceberg-warehouse/fact_geographic_analysis_parquet/metadata/'
            }
        },
        'PartitionKeys': [
            {'Name': 'country', 'Type': 'string', 'Comment': 'Partition by country'}
        ],
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'iceberg',
            'table_type': 'ICEBERG'
        }
    }
    
    return create_table_helper(glue_client, 'geographic_analysis', table_input)

def create_age_generation_analysis_table():
    """Table for Job 2: Analytics Aggregation - Age Analysis"""
    glue_client = get_glue_client()
    
    table_input = {
        'Name': 'age_generation_analysis',
        'Description': 'Age and generation analysis with engagement scoring',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'analysis_key', 'Type': 'string', 'Comment': 'Unique analysis identifier'},
                {'Name': 'age_group', 'Type': 'string', 'Comment': 'Age group category'},
                {'Name': 'generation', 'Type': 'string', 'Comment': 'Generation category'},
                {'Name': 'user_count', 'Type': 'bigint', 'Comment': 'Number of users'},
                {'Name': 'min_age', 'Type': 'int', 'Comment': 'Minimum age in group'},
                {'Name': 'max_age', 'Type': 'int', 'Comment': 'Maximum age in group'},
                {'Name': 'avg_age', 'Type': 'double', 'Comment': 'Average age'},
                {'Name': 'gender_distribution', 'Type': 'map<string,bigint>', 'Comment': 'Gender distribution'},
                {'Name': 'engagement_score', 'Type': 'double', 'Comment': 'Engagement score'},
                {'Name': 'avg_quality_score', 'Type': 'double', 'Comment': 'Average data quality'},
                {'Name': 'created_at', 'Type': 'timestamp', 'Comment': 'Record creation timestamp'},
                {'Name': 'updated_at', 'Type': 'timestamp', 'Comment': 'Last update timestamp'}
            ],
            'Location': 's3://my-amazing-app/iceberg-warehouse/fact_age_generation_analysis_parquet/',
            'InputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergInputFormat',
            'OutputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.iceberg.mr.mapreduce.IcebergSerDe'
            },
            'Parameters': {
                'table_type': 'ICEBERG',
                'metadata_location': 's3://my-amazing-app/iceberg-warehouse/fact_age_generation_analysis_parquet/metadata/'
            }
        },
        'PartitionKeys': [
            {'Name': 'generation', 'Type': 'string', 'Comment': 'Partition by generation'}
        ],
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'iceberg',
            'table_type': 'ICEBERG'
        }
    }
    
    return create_table_helper(glue_client, 'age_generation_analysis', table_input)

def create_fact_data_quality_metrics_table():
    """Table for Job 2: Analytics Aggregation - Data Quality Metrics"""
    glue_client = get_glue_client()
    
    table_input = {
        'Name': 'fact_data_quality_metrics',
        'Description': 'Data quality metrics fact table with comprehensive quality scores',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'trend_key', 'Type': 'string', 'Comment': 'Unique trend identifier'},
                {'Name': 'time_period', 'Type': 'string', 'Comment': 'Time period (monthly, quarterly, etc.)'},
                {'Name': 'period_start', 'Type': 'date', 'Comment': 'Period start date'},
                {'Name': 'period_end', 'Type': 'date', 'Comment': 'Period end date'},
                {'Name': 'registration_count', 'Type': 'bigint', 'Comment': 'Number of registrations'},
                {'Name': 'cumulative_count', 'Type': 'bigint', 'Comment': 'Cumulative registrations'},
                {'Name': 'growth_rate', 'Type': 'double', 'Comment': 'Period-over-period growth rate'},
                {'Name': 'avg_age', 'Type': 'double', 'Comment': 'Average age of registrants'},
                {'Name': 'gender_distribution', 'Type': 'map<string,bigint>', 'Comment': 'Gender distribution'},
                {'Name': 'is_anomaly', 'Type': 'boolean', 'Comment': 'Anomaly detection flag'},
                {'Name': 'anomaly_score', 'Type': 'double', 'Comment': 'Anomaly score (z-score)'},
                {'Name': 'created_at', 'Type': 'timestamp', 'Comment': 'Analysis timestamp'}
            ],
            'Location': 's3://my-amazing-app/iceberg-warehouse/fact_data_quality_metrics_parquet/',
            'InputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergInputFormat',
            'OutputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.iceberg.mr.mapreduce.IcebergSerDe'
            },
            'Parameters': {
                'table_type': 'ICEBERG',
                'metadata_location': 's3://my-amazing-app/iceberg-warehouse/fact_data_quality_metrics_parquet/metadata/'
            }
        },
        'PartitionKeys': [
            {'Name': 'year', 'Type': 'string', 'Comment': 'Year partition'},
            {'Name': 'time_period', 'Type': 'string', 'Comment': 'Time period partition'}
        ],
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'iceberg',
            'table_type': 'ICEBERG'
        }
    }
    
    return create_table_helper(glue_client, 'registration_trends', table_input)

def create_fact_email_domain_analysis_table():
    """Table for Job 2: Analytics Aggregation - Email Domain Analysis"""
    glue_client = get_glue_client()
    
    table_input = {
        'Name': 'fact_email_domain_analysis',
        'Description': 'Email domain analysis fact table with provider insights',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'expansion_key', 'Type': 'string', 'Comment': 'Unique expansion identifier'},
                {'Name': 'country', 'Type': 'string', 'Comment': 'Country name'},
                {'Name': 'state_province', 'Type': 'string', 'Comment': 'State or province'},
                {'Name': 'time_period', 'Type': 'string', 'Comment': 'Time period'},
                {'Name': 'period_start', 'Type': 'date', 'Comment': 'Period start date'},
                {'Name': 'period_end', 'Type': 'date', 'Comment': 'Period end date'},
                {'Name': 'new_users', 'Type': 'bigint', 'Comment': 'New users in period'},
                {'Name': 'total_users', 'Type': 'bigint', 'Comment': 'Total users in location'},
                {'Name': 'penetration_rate', 'Type': 'double', 'Comment': 'Market penetration rate'},
                {'Name': 'growth_rate', 'Type': 'double', 'Comment': 'Growth rate'},
                {'Name': 'is_new_market', 'Type': 'boolean', 'Comment': 'New market flag'},
                {'Name': 'expansion_score', 'Type': 'double', 'Comment': 'Expansion opportunity score'},
                {'Name': 'created_at', 'Type': 'timestamp', 'Comment': 'Analysis timestamp'}
            ],
            'Location': 's3://my-amazing-app/iceberg-warehouse/fact_email_domain_analysis_parquet/',
            'InputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergInputFormat',
            'OutputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.iceberg.mr.mapreduce.IcebergSerDe'
            },
            'Parameters': {
                'table_type': 'ICEBERG',
                'metadata_location': 's3://my-amazing-app/iceberg-warehouse/fact_email_domain_analysis_parquet/metadata/'
            }
        },
        'PartitionKeys': [
            {'Name': 'country', 'Type': 'string', 'Comment': 'Country partition'},
            {'Name': 'year', 'Type': 'string', 'Comment': 'Year partition'}
        ],
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'iceberg',
            'table_type': 'ICEBERG'
        }
    }
    
    return create_table_helper(glue_client, 'geographic_expansion', table_input)

def create_fact_email_provider_analysis_table():
    """Table for Job 2: Analytics Aggregation - Email Provider Analysis"""
    glue_client = get_glue_client()
    
    table_input = {
        'Name': 'fact_email_provider_analysis',
        'Description': 'Email provider analysis fact table with market share insights',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'quality_key', 'Type': 'string', 'Comment': 'Unique quality identifier'},
                {'Name': 'time_period', 'Type': 'string', 'Comment': 'Time period'},
                {'Name': 'period_start', 'Type': 'date', 'Comment': 'Period start date'},
                {'Name': 'period_end', 'Type': 'date', 'Comment': 'Period end date'},
                {'Name': 'avg_quality_score', 'Type': 'double', 'Comment': 'Average quality score'},
                {'Name': 'min_quality_score', 'Type': 'double', 'Comment': 'Minimum quality score'},
                {'Name': 'max_quality_score', 'Type': 'double', 'Comment': 'Maximum quality score'},
                {'Name': 'quality_trend', 'Type': 'string', 'Comment': 'Quality trend (Improving, Declining, Stable)'},
                {'Name': 'records_processed', 'Type': 'bigint', 'Comment': 'Total records processed'},
                {'Name': 'high_quality_records', 'Type': 'bigint', 'Comment': 'High quality records (>0.8)'},
                {'Name': 'low_quality_records', 'Type': 'bigint', 'Comment': 'Low quality records (<0.5)'},
                {'Name': 'quality_issues', 'Type': 'array<string>', 'Comment': 'List of quality issues detected'},
                {'Name': 'is_anomaly', 'Type': 'boolean', 'Comment': 'Quality anomaly flag'},
                {'Name': 'anomaly_score', 'Type': 'double', 'Comment': 'Quality anomaly score'},
                {'Name': 'created_at', 'Type': 'timestamp', 'Comment': 'Analysis timestamp'}
            ],
            'Location': 's3://my-amazing-app/iceberg-warehouse/fact_email_provider_analysis_parquet/',
            'InputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergInputFormat',
            'OutputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.iceberg.mr.mapreduce.IcebergSerDe'
            },
            'Parameters': {
                'table_type': 'ICEBERG',
                'metadata_location': 's3://my-amazing-app/iceberg-warehouse/fact_email_provider_analysis_parquet/metadata/'
            }
        },
        'PartitionKeys': [
            {'Name': 'year', 'Type': 'string', 'Comment': 'Year partition'},
            {'Name': 'quality_trend', 'Type': 'string', 'Comment': 'Quality trend partition'}
        ],
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'iceberg',
            'table_type': 'ICEBERG'
        }
    }
    
    return create_table_helper(glue_client, 'fact_email_provider_analysis', table_input)

def create_fact_quality_by_segment_table():
    """Table for Job 2: Analytics Aggregation - Quality by Segment"""
    glue_client = get_glue_client()
    
    table_input = {
        'Name': 'fact_quality_by_segment',
        'Description': 'Quality analysis by demographic segments',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'segment_key', 'Type': 'string', 'Comment': 'Unique segment identifier'},
                {'Name': 'age_group', 'Type': 'string', 'Comment': 'Age group category'},
                {'Name': 'generation', 'Type': 'string', 'Comment': 'Generation category'},
                {'Name': 'gender', 'Type': 'string', 'Comment': 'Gender'},
                {'Name': 'user_count', 'Type': 'bigint', 'Comment': 'Number of users in segment'},
                {'Name': 'avg_segment_quality', 'Type': 'double', 'Comment': 'Average quality score for segment'},
                {'Name': 'quality_distribution', 'Type': 'map<string,bigint>', 'Comment': 'Quality score distribution'},
                {'Name': 'email_quality_score', 'Type': 'double', 'Comment': 'Email validation quality'},
                {'Name': 'phone_quality_score', 'Type': 'double', 'Comment': 'Phone validation quality'},
                {'Name': 'created_at', 'Type': 'timestamp', 'Comment': 'Analysis timestamp'}
            ],
            'Location': 's3://my-amazing-app/iceberg-warehouse/fact_quality_by_segment_parquet/',
            'InputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergInputFormat',
            'OutputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.iceberg.mr.mapreduce.IcebergSerDe'
            },
            'Parameters': {
                'table_type': 'ICEBERG',
                'metadata_location': 's3://my-amazing-app/iceberg-warehouse/fact_quality_by_segment_parquet/metadata/'
            }
        },
        'PartitionKeys': [
            {'Name': 'age_group', 'Type': 'string', 'Comment': 'Age group partition'}
        ],
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'iceberg',
            'table_type': 'ICEBERG'
        }
    }
    
    return create_table_helper(glue_client, 'fact_quality_by_segment', table_input)

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

def verify_all_tables():
    """Verify all tables were created successfully"""
    glue_client = get_glue_client()
    
    expected_tables = [
        'users_transformed',
        'data_quality_summary',
        'users_cleaned',
        'user_demographics', 
        'geographic_analysis',
        'age_generation_analysis',
        'fact_data_quality_metrics',
        'fact_email_domain_analysis',
        'fact_email_provider_analysis',
        'fact_quality_by_segment'
    ]
    
    print("\n🔍 Verifying all tables...")
    success_count = 0
    
    for table_name in expected_tables:
        try:
            response = glue_client.get_table(
                DatabaseName='data_pipeline_db',
                Name=table_name
            )
            print(f"✅ {table_name}: {len(response['Table']['StorageDescriptor']['Columns'])} columns")
            success_count += 1
        except Exception as e:
            print(f"❌ {table_name}: Not found or error - {str(e)}")
    
    print(f"\n📊 Summary: {success_count}/{len(expected_tables)} tables verified successfully")
    return success_count == len(expected_tables)

def main():
    """Main function to create all catalog tables"""
    print("🚀 Setting Up All Glue Catalog Tables")
    print("=" * 60)
    
    # Create database
    if not create_database():
        print("❌ Failed to create database. Exiting.")
        sys.exit(1)
    
    print("\n📋 Creating tables for all Glue jobs...")
    
    # Track success
    tables_created = []
    
    # Job 1: Raw Data Transformation
    print("\n🔄 Job 1: Raw Data Transformation Tables")
    if create_users_transformed_table():
        tables_created.append('users_transformed')
    if create_data_quality_summary_table():
        tables_created.append('data_quality_summary')
    if create_users_cleaned_table():
        tables_created.append('users_cleaned')
    
    # Job 2: Analytics Aggregation  
    print("\n🔄 Job 2: Analytics Aggregation Tables")
    if create_user_demographics_table():
        tables_created.append('user_demographics')
    if create_geographic_analysis_table():
        tables_created.append('geographic_analysis')
    if create_age_generation_analysis_table():
        tables_created.append('age_generation_analysis')
    if create_fact_data_quality_metrics_table():
        tables_created.append('fact_data_quality_metrics')
    if create_fact_email_domain_analysis_table():
        tables_created.append('fact_email_domain_analysis')
    if create_fact_email_provider_analysis_table():
        tables_created.append('fact_email_provider_analysis')
    if create_fact_quality_by_segment_table():
        tables_created.append('fact_quality_by_segment')
    
    # Verify all tables
    print("\n" + "=" * 60)
    if verify_all_tables():
        print("\n🎉 SUCCESS! All catalog tables are ready!")
        print(f"\n📊 Tables Created: {len(tables_created)}")
        for table in tables_created:
            print(f"  ✅ {table}")
        
        print("\n🔗 Next Steps:")
        print("1. Go to AWS Glue Console → Data Catalog → Tables")
        print("2. Verify all tables in 'data_pipeline_db' database")
        print("3. Run your Glue jobs in sequence:")
        print("   • data-pipeline-raw-transformation")
        print("   • data-pipeline-analytics-aggregation")
        print("4. Query tables in Amazon Athena")
        
        print("\n🔗 Useful Links:")
        print("• Glue Console: https://console.aws.amazon.com/glue/")
        print("• Athena Console: https://console.aws.amazon.com/athena/")
        print("• S3 Warehouse: s3://my-amazing-app/iceberg-warehouse/")
        
    else:
        print("\n⚠️ Some tables failed to create. Check the errors above.")
        sys.exit(1)

if __name__ == "__main__":
    main() 