#!/usr/bin/env python3

"""
AWS Glue Jobs Configuration and Deployment Script
================================================

This script helps configure, deploy, and manage AWS Glue jobs for the data pipeline.
It includes job definitions, IAM roles, and deployment automation.
"""

import boto3
import json
import os
from typing import Dict, List, Any
from datetime import datetime

class GlueJobManager:
    """Manages AWS Glue jobs for the data pipeline"""
    
    def __init__(self, region_name: str = 'us-east-1'):
        """Initialize the Glue job manager"""
        self.glue_client = boto3.client('glue', region_name=region_name)
        self.iam_client = boto3.client('iam', region_name=region_name)
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.region = region_name
        
    def create_glue_service_role(self, role_name: str = 'DataPipelineGlueRole') -> str:
        """Create IAM role for Glue jobs"""
        print(f"🔐 Creating Glue service role: {role_name}")
        
        # Trust policy for Glue service
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "glue.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        
        try:
            # Create role
            response = self.iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description='IAM role for Data Pipeline Glue jobs with Iceberg support'
            )
            
            # Attach AWS managed policies
            managed_policies = [
                'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole',
                'arn:aws:iam::aws:policy/AmazonS3FullAccess',
                'arn:aws:iam::aws:policy/CloudWatchLogsFullAccess'
            ]
            
            for policy_arn in managed_policies:
                self.iam_client.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy_arn
                )
            
            # Create custom policy for Iceberg and advanced features
            custom_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "glue:*",
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject",
                            "s3:ListBucket",
                            "s3:GetBucketLocation",
                            "s3:ListAllMyBuckets",
                            "s3:GetBucketAcl",
                            "lakeformation:GetDataAccess",
                            "lakeformation:GrantPermissions",
                            "lakeformation:BatchGrantPermissions",
                            "lakeformation:RevokePermissions",
                            "lakeformation:BatchRevokePermissions",
                            "lakeformation:ListPermissions"
                        ],
                        "Resource": "*"
                    }
                ]
            }
            
            self.iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName='DataPipelineGlueCustomPolicy',
                PolicyDocument=json.dumps(custom_policy)
            )
            
            role_arn = response['Role']['Arn']
            print(f"✅ Created Glue service role: {role_arn}")
            return role_arn
            
        except self.iam_client.exceptions.EntityAlreadyExistsException:
            # Role already exists, get its ARN
            response = self.iam_client.get_role(RoleName=role_name)
            role_arn = response['Role']['Arn']
            print(f"✅ Using existing Glue service role: {role_arn}")
            return role_arn
    
    def upload_job_scripts(self, s3_bucket: str, s3_prefix: str = 'glue-scripts/') -> Dict[str, str]:
        """Upload Glue job scripts to S3"""
        print(f"📤 Uploading Glue job scripts to s3://{s3_bucket}/{s3_prefix}")
        
        job_scripts = {
            '01_raw_data_transformation.py': 'Raw Data Transformation',
            '02_analytics_aggregation.py': 'Analytics Aggregation',
            '03_time_series_analysis.py': 'Time Series Analysis'
        }
        
        uploaded_scripts = {}
        
        for script_file, description in job_scripts.items():
            local_path = f"glue_jobs/{script_file}"
            s3_key = f"{s3_prefix}{script_file}"
            
            try:
                # Upload script to S3
                self.s3_client.upload_file(local_path, s3_bucket, s3_key)
                s3_path = f"s3://{s3_bucket}/{s3_key}"
                uploaded_scripts[script_file] = s3_path
                print(f"✅ Uploaded {script_file} to {s3_path}")
                
            except Exception as e:
                print(f"❌ Failed to upload {script_file}: {str(e)}")
                
        return uploaded_scripts
    
    def get_job_definitions(self, role_arn: str, script_paths: Dict[str, str], 
                           s3_input_path: str, s3_output_path: str, 
                           catalog_database: str) -> List[Dict[str, Any]]:
        """Get Glue job definitions"""
        
        # Common job configuration
        common_config = {
            'Role': role_arn,
            'ExecutionProperty': {'MaxConcurrentRuns': 1},
            'Command': {
                'Name': 'glueetl',
                'PythonVersion': '3'
            },
            'DefaultArguments': {
                '--job-language': 'python',
                '--job-bookmark-option': 'job-bookmark-enable',
                '--enable-metrics': 'true',
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-spark-ui': 'true',
                '--spark-event-logs-path': f'{s3_output_path}/spark-logs/',
                '--S3_INPUT_PATH': s3_input_path,
                '--S3_OUTPUT_PATH': s3_output_path,
                '--CATALOG_DATABASE': catalog_database,
                '--AWS_REGION': self.region,
                # Iceberg configuration
                '--datalake-formats': 'iceberg',
                '--additional-python-modules': 'pyiceberg'
            },
            'MaxRetries': 2,
            'Timeout': 2880,  # 48 hours
            'GlueVersion': '4.0',
            'NumberOfWorkers': 5,
            'WorkerType': 'G.1X'
        }
        
        job_definitions = [
            {
                'Name': 'data-pipeline-raw-transformation',
                'Description': 'Transform raw user data with validation and enrichment',
                'Command': {
                    **common_config['Command'],
                    'ScriptLocation': script_paths.get('01_raw_data_transformation.py', '')
                },
                **{k: v for k, v in common_config.items() if k != 'Command'},
                'Tags': {
                    'Environment': 'production',
                    'Project': 'data-pipeline',
                    'JobType': 'transformation'
                }
            },
            {
                'Name': 'data-pipeline-analytics-aggregation',
                'Description': 'Create analytics aggregations and dimensional tables',
                'Command': {
                    **common_config['Command'],
                    'ScriptLocation': script_paths.get('02_analytics_aggregation.py', '')
                },
                **{k: v for k, v in common_config.items() if k != 'Command'},
                'Tags': {
                    'Environment': 'production',
                    'Project': 'data-pipeline',
                    'JobType': 'analytics'
                }
            },
            {
                'Name': 'data-pipeline-time-series-analysis',
                'Description': 'Perform time series analysis and trend detection',
                'Command': {
                    **common_config['Command'],
                    'ScriptLocation': script_paths.get('03_time_series_analysis.py', '')
                },
                **{k: v for k, v in common_config.items() if k != 'Command'},
                'Tags': {
                    'Environment': 'production',
                    'Project': 'data-pipeline',
                    'JobType': 'analysis'
                }
            }
        ]
        
        return job_definitions
    
    def create_glue_jobs(self, job_definitions: List[Dict[str, Any]]) -> List[str]:
        """Create or update Glue jobs"""
        created_jobs = []
        
        for job_def in job_definitions:
            job_name = job_def['Name']
            print(f"🔧 Creating/updating Glue job: {job_name}")
            
            try:
                # Check if job exists
                try:
                    self.glue_client.get_job(JobName=job_name)
                    # Job exists, update it
                    job_update = {k: v for k, v in job_def.items() if k != 'Name'}
                    self.glue_client.update_job(
                        JobName=job_name,
                        JobUpdate=job_update
                    )
                    print(f"✅ Updated existing job: {job_name}")
                    
                except self.glue_client.exceptions.EntityNotFoundException:
                    # Job doesn't exist, create it
                    self.glue_client.create_job(**job_def)
                    print(f"✅ Created new job: {job_name}")
                
                created_jobs.append(job_name)
                
            except Exception as e:
                print(f"❌ Failed to create/update job {job_name}: {str(e)}")
                
        return created_jobs
    
    def create_glue_database(self, database_name: str) -> bool:
        """Create Glue catalog database"""
        print(f"🗄️ Creating Glue catalog database: {database_name}")
        
        try:
            self.glue_client.create_database(
                DatabaseInput={
                    'Name': database_name,
                    'Description': 'Database for data pipeline Iceberg tables'
                }
            )
            print(f"✅ Created database: {database_name}")
            return True
            
        except self.glue_client.exceptions.AlreadyExistsException:
            print(f"✅ Database already exists: {database_name}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to create database: {str(e)}")
            return False
    
    def create_glue_workflow(self, workflow_name: str, job_names: List[str]) -> bool:
        """Create Glue workflow to orchestrate jobs"""
        print(f"🔄 Creating Glue workflow: {workflow_name}")
        
        try:
            # Create workflow
            self.glue_client.create_workflow(
                Name=workflow_name,
                Description='Data pipeline workflow for sequential job execution',
                Tags={
                    'Environment': 'production',
                    'Project': 'data-pipeline'
                }
            )
            
            # Create triggers for sequential execution
            triggers = []
            
            # Start trigger
            triggers.append({
                'Name': f'{workflow_name}-start-trigger',
                'Type': 'ON_DEMAND',
                'Actions': [
                    {
                        'JobName': job_names[0],
                        'Arguments': {}
                    }
                ]
            })
            
            # Sequential triggers
            for i in range(1, len(job_names)):
                triggers.append({
                    'Name': f'{workflow_name}-trigger-{i}',
                    'Type': 'CONDITIONAL',
                    'Predicate': {
                        'Conditions': [
                            {
                                'LogicalOperator': 'EQUALS',
                                'JobName': job_names[i-1],
                                'State': 'SUCCEEDED'
                            }
                        ]
                    },
                    'Actions': [
                        {
                            'JobName': job_names[i],
                            'Arguments': {}
                        }
                    ]
                })
            
            # Create triggers
            for trigger in triggers:
                self.glue_client.create_trigger(
                    WorkflowName=workflow_name,
                    **trigger
                )
            
            print(f"✅ Created workflow: {workflow_name}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to create workflow: {str(e)}")
            return False
    
    def deploy_complete_pipeline(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Deploy complete Glue pipeline"""
        print("🚀 Deploying complete Glue pipeline...")
        
        results = {
            'role_arn': None,
            'uploaded_scripts': {},
            'created_jobs': [],
            'database_created': False,
            'workflow_created': False
        }
        
        try:
            # 1. Create IAM role
            results['role_arn'] = self.create_glue_service_role(config['role_name'])
            
            # 2. Upload scripts
            results['uploaded_scripts'] = self.upload_job_scripts(
                config['s3_bucket'], config['s3_scripts_prefix']
            )
            
            # 3. Create database
            results['database_created'] = self.create_glue_database(config['catalog_database'])
            
            # 4. Create jobs
            job_definitions = self.get_job_definitions(
                results['role_arn'],
                results['uploaded_scripts'],
                config['s3_input_path'],
                config['s3_output_path'],
                config['catalog_database']
            )
            
            results['created_jobs'] = self.create_glue_jobs(job_definitions)
            
            # 5. Create workflow
            if results['created_jobs']:
                results['workflow_created'] = self.create_glue_workflow(
                    config['workflow_name'], results['created_jobs']
                )
            
            print("✅ Complete Glue pipeline deployed successfully!")
            return results
            
        except Exception as e:
            print(f"❌ Pipeline deployment failed: {str(e)}")
            return results

def main():
    """Main deployment function"""
    print("🚀 AWS Glue Jobs Deployment Script")
    print("=" * 50)
    
    # Configuration
    config = {
        'role_name': 'DataPipelineGlueRole',
        's3_bucket': 'my-amazing-app',  # Replace with your bucket
        's3_scripts_prefix': 'glue-scripts/',
        's3_input_path': 's3://my-amazing-app',
        's3_output_path': 's3://my-amazing-app/iceberg-warehouse',
        'catalog_database': 'data_pipeline_db',
        'workflow_name': 'data-pipeline-workflow',
        'region': 'us-east-1'
    }
    
    # Initialize manager
    manager = GlueJobManager(region_name=config['region'])
    
    # Deploy pipeline
    results = manager.deploy_complete_pipeline(config)
    
    # Print summary
    print("\n" + "=" * 50)
    print("🎉 DEPLOYMENT SUMMARY")
    print("=" * 50)
    print(f"IAM Role: {results['role_arn']}")
    print(f"Scripts Uploaded: {len(results['uploaded_scripts'])}")
    print(f"Jobs Created: {len(results['created_jobs'])}")
    print(f"Database Created: {results['database_created']}")
    print(f"Workflow Created: {results['workflow_created']}")
    
    if results['created_jobs']:
        print("\n📋 Created Jobs:")
        for job in results['created_jobs']:
            print(f"  ✅ {job}")
    
    print("\n🎯 Next Steps:")
    print("1. Verify jobs in AWS Glue Console")
    print("2. Test jobs individually")
    print("3. Run complete workflow")
    print("4. Monitor job execution and logs")
    print("5. Query Iceberg tables in Athena/Snowflake")

if __name__ == "__main__":
    main() 