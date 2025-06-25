#!/usr/bin/env python3

"""
Run Glue Jobs Script
===================

This script helps you run the Glue jobs with proper parameters.
"""

import boto3
import json
import sys
import time
from datetime import datetime

def get_glue_client():
    """Get Glue client"""
    return boto3.client('glue')

def run_glue_job(job_name: str, job_parameters: dict):
    """Run a Glue job with parameters"""
    glue_client = get_glue_client()
    
    print(f"Starting Glue job: {job_name}")
    print(f"Parameters: {json.dumps(job_parameters, indent=2)}")
    
    try:
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=job_parameters
        )
        
        job_run_id = response['JobRunId']
        print(f"✅ Job started successfully!")
        print(f"🆔 Job Run ID: {job_run_id}")
        
        return job_run_id
        
    except Exception as e:
        print(f"❌ Failed to start job: {str(e)}")
        return None

def wait_for_job_completion(job_name: str, job_run_id: str, timeout_minutes: int = 30):
    """Wait for job completion"""
    glue_client = get_glue_client()
    
    print(f"Waiting for job completion (timeout: {timeout_minutes} minutes)...")
    
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    
    while True:
        try:
            response = glue_client.get_job_run(
                JobName=job_name,
                RunId=job_run_id
            )
            
            job_run = response['JobRun']
            job_state = job_run['JobRunState']
            
            elapsed_time = int(time.time() - start_time)
            print(f"Status: {job_state} (elapsed: {elapsed_time}s)")
            
            if job_state in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
                if job_state == 'SUCCEEDED':
                    print(f"✅ Job completed successfully!")
                    if 'ExecutionTime' in job_run:
                        print(f"Execution Time: {job_run['ExecutionTime']} seconds")
                else:
                    print(f"❌ Job failed with state: {job_state}")
                    if 'ErrorMessage' in job_run:
                        print(f"Error: {job_run['ErrorMessage']}")
                
                return job_state == 'SUCCEEDED'
            
            if elapsed_time > timeout_seconds:
                print(f"Job timed out after {timeout_minutes} minutes")
                return False
            
            time.sleep(30)  # Wait 30 seconds before checking again
            
        except Exception as e:
            print(f"❌ Error checking job status: {str(e)}")
            return False

def main():
    """Main function to run Glue jobs"""
    print("Glue Jobs Runner")
    print("=" * 50)
    
    # Job parameters
    job_params = {
        '--S3_INPUT_PATH': 's3://my-amazing-app',
        '--S3_OUTPUT_PATH': 's3://my-amazing-app/iceberg-warehouse',
        '--CATALOG_DATABASE': 'data_pipeline_db',
        '--CATALOG_TABLE': 'users',
        '--AWS_REGION': 'eu-west-3'
    }
    
    # Jobs to run in sequence
    jobs_to_run = [
        {
            'name': 'data-pipeline-raw-transformation',
            'description': 'Raw Data Transformation',
            'params': job_params
        },
        {
            'name': 'data-pipeline-analytics-aggregation', 
            'description': 'Analytics Aggregation',
            'params': job_params
        }
    ]
    
    print(f"📋 Jobs to run: {len(jobs_to_run)}")
    for i, job in enumerate(jobs_to_run, 1):
        print(f"  {i}. {job['description']} ({job['name']})")
    
    # Ask user what to do
    print(f"\nWhat would you like to do?")
    print("1. Run all jobs in sequence")
    print("2. Run job 1 only (Raw Transformation)")
    print("3. Run job 2 only (Analytics Aggregation)")
    print("4. Just show job parameters")
    print("5. Exit")
    
    try:
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == '5':
            print("Goodbye!")
            return
        elif choice == '4':
            print("\nJob Parameters:")
            print(json.dumps(job_params, indent=2))
            return
        elif choice in ['1', '2', '3']:
            if choice == '1':
                selected_jobs = jobs_to_run
            elif choice == '2':
                selected_jobs = [jobs_to_run[0]]
            else:  # choice == '3'
                selected_jobs = [jobs_to_run[1]]
            
            print(f"\nRunning {len(selected_jobs)} job(s)...")
            
            all_successful = True
            for i, job in enumerate(selected_jobs, 1):
                print(f"\n{'='*60}")
                print(f"🔄 Job {i}/{len(selected_jobs)}: {job['description']}")
                print(f"{'='*60}")
                
                job_run_id = run_glue_job(job['name'], job['params'])
                
                if job_run_id:
                    success = wait_for_job_completion(job['name'], job_run_id)
                    if not success:
                        all_successful = False
                        print(f"❌ Job {job['name']} failed!")
                        
                        # Ask if user wants to continue
                        if i < len(selected_jobs):
                            continue_choice = input("Continue with next job? (y/N): ").strip().lower()
                            if continue_choice != 'y':
                                break
                else:
                    all_successful = False
                    break
            
            print(f"\n{'='*60}")
            if all_successful:
                print("\t All jobs completed successfully!")
                print("\n Next Steps:")
                print("1. Check your S3 bucket for the Iceberg tables")
                print("2. Verify tables in AWS Glue Data Catalog")
                print("3. Try your Snowflake CREATE ICEBERG TABLE command")
                print("4. Run analytics queries in Snowflake")
            else:
                print("⚠️ Some jobs failed. Check the AWS Glue console for details.")
                
        else:
            print("❌ Invalid choice. Please enter 1-5.")
            
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")

if __name__ == "__main__":
    main() 