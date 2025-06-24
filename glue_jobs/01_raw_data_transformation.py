"""
AWS Glue Job: Raw User Data Transformation to Iceberg
=====================================================

This Glue job processes raw user data from S3, performs data quality checks,
enriches the data with additional fields, and saves to Iceberg tables.

Features:
- Data validation and cleansing
- Age calculation and categorization
- Address parsing and geocoding preparation
- Email domain extraction
- Phone number standardization
- Data quality metrics collection
- Iceberg table with schema evolution support
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from datetime import datetime, date

# Initialize Glue context
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_INPUT_PATH',
    'S3_OUTPUT_PATH', 
    'CATALOG_DATABASE',
    'CATALOG_TABLE',
    'AWS_REGION'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define catalog tables based on the base table name
CATALOG_TABLES = {
    'users_transformed': f"{args['CATALOG_TABLE']}_transformed",
    'data_quality_summary': f"{args['CATALOG_TABLE']}_quality_summary"
}

def validate_and_cleanse_data(df: DataFrame) -> DataFrame:
    """
    Perform data validation and cleansing on raw user data
    """
    print("🔍 Starting data validation and cleansing...")
    
    # Add data quality flags
    df_validated = df.withColumn("data_quality_score", lit(100)) \
        .withColumn("quality_issues", array())
    
    # Check for missing critical fields
    df_validated = df_validated.withColumn(
        "data_quality_score",
        when(col("id").isNull() | (col("id") == ""), col("data_quality_score") - 20)
        .otherwise(col("data_quality_score"))
    ).withColumn(
        "quality_issues",
        when(col("id").isNull() | (col("id") == ""), 
             array_union(col("quality_issues"), array(lit("missing_id"))))
        .otherwise(col("quality_issues"))
    )
    
    # Validate email format
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    df_validated = df_validated.withColumn(
        "email_valid",
        when(col("email").rlike(email_pattern), True).otherwise(False)
    ).withColumn(
        "data_quality_score",
        when(~col("email_valid"), col("data_quality_score") - 15)
        .otherwise(col("data_quality_score"))
    ).withColumn(
        "quality_issues",
        when(~col("email_valid"),
             array_union(col("quality_issues"), array(lit("invalid_email"))))
        .otherwise(col("quality_issues"))
    )
    
    # Validate phone numbers (basic check)
    df_validated = df_validated.withColumn(
        "phone_valid",
        when(col("phone").isNull() | (length(col("phone")) < 10), False).otherwise(True)
    ).withColumn(
        "data_quality_score",
        when(~col("phone_valid"), col("data_quality_score") - 10)
        .otherwise(col("data_quality_score"))
    ).withColumn(
        "quality_issues",
        when(~col("phone_valid"),
             array_union(col("quality_issues"), array(lit("invalid_phone"))))
        .otherwise(col("quality_issues"))
    )
    
    # Clean and standardize names
    df_validated = df_validated.withColumn(
        "first_name_clean",
        initcap(trim(col("first_name")))
    ).withColumn(
        "last_name_clean", 
        initcap(trim(col("last_name")))
    ).withColumn(
        "full_name",
        concat_ws(" ", col("first_name_clean"), col("last_name_clean"))
    )
    
    print(f"✅ Data validation completed")
    return df_validated

def enrich_user_data(df: DataFrame) -> DataFrame:
    """
    Enrich user data with calculated fields and derived attributes
    """
    print("🔧 Starting data enrichment...")
    
    # Calculate age from date of birth
    df_enriched = df.withColumn(
        "birth_date",
        to_date(to_timestamp(col("dob"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    ).withColumn(
        "age_years",
        floor(datediff(current_date(), col("birth_date")) / 365.25)
    )
    
    # Create age categories
    df_enriched = df_enriched.withColumn(
        "age_group",
        when(col("age_years") < 25, "18-24")
        .when(col("age_years") < 35, "25-34")
        .when(col("age_years") < 45, "35-44")
        .when(col("age_years") < 55, "45-54")
        .when(col("age_years") < 65, "55-64")
        .otherwise("65+")
    ).withColumn(
        "generation",
        when(col("age_years") < 27, "Gen Z")
        .when(col("age_years") < 43, "Millennial")
        .when(col("age_years") < 59, "Gen X")
        .otherwise("Boomer")
    )
    
    # Extract email domain and categorize
    df_enriched = df_enriched.withColumn(
        "email_domain",
        regexp_extract(col("email"), "@([^.]+\\..*)", 1)
    ).withColumn(
        "email_provider_type",
        when(col("email_domain").isin("gmail.com", "yahoo.com", "hotmail.com", "outlook.com"), "Personal")
        .when(col("email_domain").contains("edu"), "Educational")
        .when(col("email_domain").contains("gov"), "Government")
        .otherwise("Business")
    )
    
    # Parse address components
    df_enriched = df_enriched.withColumn(
        "address_parts",
        split(col("address"), ",")
    ).withColumn(
        "street_address",
        trim(col("address_parts")[0])
    ).withColumn(
        "city",
        when(size(col("address_parts")) > 1, trim(col("address_parts")[1])).otherwise(lit("Unknown"))
    ).withColumn(
        "state_country",
        when(size(col("address_parts")) > 2, trim(col("address_parts")[2])).otherwise(lit("Unknown"))
    )
    
    # Extract country (assuming last part after last space)
    df_enriched = df_enriched.withColumn(
        "country",
        regexp_extract(col("state_country"), r"\s+([A-Za-z\s]+)$", 1)
    ).withColumn(
        "country",
        when(col("country") == "", "Unknown").otherwise(col("country"))
    )
    
    # Standardize phone numbers
    df_enriched = df_enriched.withColumn(
        "phone_clean",
        regexp_replace(col("phone"), r"[^\d]", "")
    ).withColumn(
        "phone_formatted",
        when(length(col("phone_clean")) == 10,
             concat(lit("("), substring(col("phone_clean"), 1, 3), lit(") "),
                   substring(col("phone_clean"), 4, 3), lit("-"),
                   substring(col("phone_clean"), 7, 4)))
        .otherwise(col("phone"))
    )
    
    # Calculate registration tenure
    df_enriched = df_enriched.withColumn(
        "registration_date",
        to_date(to_timestamp(col("registered_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    ).withColumn(
        "account_age_days",
        datediff(current_date(), col("registration_date"))
    ).withColumn(
        "account_tenure_category",
        when(col("account_age_days") < 30, "New")
        .when(col("account_age_days") < 365, "Regular")
        .otherwise("Veteran")
    )
    
    # Add processing metadata
    df_enriched = df_enriched.withColumn(
        "processed_at",
        current_timestamp()
    ).withColumn(
        "processing_date",
        current_date()
    ).withColumn(
        "data_source",
        lit("randomuser_api")
    ).withColumn(
        "glue_job_name",
        lit(args['JOB_NAME'])
    )
    
    print(f"✅ Data enrichment completed")
    return df_enriched

def create_data_quality_summary(df: DataFrame) -> DataFrame:
    """
    Create a summary of data quality metrics
    """
    print("📊 Creating data quality summary...")
    
    total_records = df.count()
    
    quality_summary = df.agg(
        count("*").alias("total_records"),
        sum(when(col("data_quality_score") >= 80, 1).otherwise(0)).alias("high_quality_records"),
        sum(when(col("data_quality_score").between(50, 79), 1).otherwise(0)).alias("medium_quality_records"),
        sum(when(col("data_quality_score") < 50, 1).otherwise(0)).alias("low_quality_records"),
        avg("data_quality_score").alias("avg_quality_score"),
        sum(when(col("email_valid"), 1).otherwise(0)).alias("valid_emails"),
        sum(when(col("phone_valid"), 1).otherwise(0)).alias("valid_phones"),
        countDistinct("email_domain").alias("unique_email_domains"),
        countDistinct("country").alias("unique_countries")
    ).withColumn(
        "quality_check_timestamp",
        current_timestamp()
    ).withColumn(
        "processing_date",
        current_date()
    ).withColumn(
        "glue_job_name",
        lit(args['JOB_NAME'])
    )
    
    print(f"✅ Data quality summary created")
    return quality_summary

def save_to_iceberg_table(df: DataFrame, table_name: str, write_mode: str = "append"):
    """
    Save DataFrame to Iceberg table with proper configuration
    """
    print(f"💾 Saving data to Iceberg table: {table_name}")
    
    # Create table identifier
    full_table_name = f"glue_catalog.{args['CATALOG_DATABASE']}.{table_name}"
    
    try:
        # Write to Iceberg table
        df.write.format("iceberg").mode(write_mode).saveAsTable(full_table_name)
        print(f"✅ Successfully saved to Iceberg table: {full_table_name}")
        
        # Print table info
        spark.sql(f"DESCRIBE TABLE {full_table_name}").show(truncate=False)
        
    except Exception as e:
        print(f"❌ Error saving to Iceberg table: {str(e)}")
        # Fallback to Parquet if Iceberg fails
        fallback_path = f"{args['S3_OUTPUT_PATH']}/{table_name}_parquet/"
        print(f"💾 Falling back to Parquet format at: {fallback_path}")
        df.write.mode(write_mode).parquet(fallback_path)

def main():
    """
    Main transformation pipeline
    """
    print("🚀 Starting Raw User Data Transformation Job")
    print(f"📥 Input Path: {args['S3_INPUT_PATH']}")
    print(f"📤 Output Path: {args['S3_OUTPUT_PATH']}")
    print(f"🗄️ Database: {args['CATALOG_DATABASE']}")
    print(f"📋 Target Tables:")
    for key, table_name in CATALOG_TABLES.items():
        print(f"  • {key}: {table_name}")
    
    try:
        # Read raw data from S3
        print("📖 Reading raw data from S3...")
        raw_df = spark.read.parquet(f"{args['S3_INPUT_PATH']}/users/raw/parquet/")
        
        print(f"📊 Raw data count: {raw_df.count()} records")
        raw_df.printSchema()
        
        # Step 1: Validate and cleanse data
        validated_df = validate_and_cleanse_data(raw_df)
        
        # Step 2: Enrich data with additional fields
        enriched_df = enrich_user_data(validated_df)
        
        # Step 3: Create final clean dataset
        final_df = enriched_df.select(
            # Original fields (cleaned)
            col("id"),
            col("first_name_clean").alias("first_name"),
            col("last_name_clean").alias("last_name"),
            col("full_name"),
            col("gender"),
            col("email"),
            col("email_domain"),
            col("email_provider_type"),
            col("email_valid"),
            col("username"),
            col("phone_formatted").alias("phone"),
            col("phone_valid"),
            
            # Address fields
            col("address"),
            col("street_address"),
            col("city"),
            col("country"),
            col("post_code"),
            
            # Date and age fields
            col("birth_date"),
            col("age_years"),
            col("age_group"),
            col("generation"),
            col("registration_date"),
            col("account_age_days"),
            col("account_tenure_category"),
            
            # Data quality fields
            col("data_quality_score"),
            col("quality_issues"),
            
            # Metadata
            col("picture"),
            col("processed_at"),
            col("processing_date"),
            col("data_source"),
            col("glue_job_name")
        )
        
        print(f"📊 Final transformed data count: {final_df.count()} records")
        final_df.printSchema()
        
        # Step 4: Save to Iceberg table
        save_to_iceberg_table(final_df, CATALOG_TABLES['users_transformed'], "overwrite")
        
        # Step 5: Create and save data quality summary
        quality_summary_df = create_data_quality_summary(enriched_df)
        quality_summary_df.show(truncate=False)
        save_to_iceberg_table(quality_summary_df, CATALOG_TABLES['data_quality_summary'], "append")
        
        # Step 6: Create sample views for analysis
        final_df.createOrReplaceTempView("users_transformed")
        
        # Sample analytics queries
        print("📈 Sample Analytics:")
        
        # Age distribution
        spark.sql("""
            SELECT age_group, generation, COUNT(*) as count,
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
            FROM users_transformed 
            GROUP BY age_group, generation
            ORDER BY age_group
        """).show()
        
        # Data quality distribution
        spark.sql("""
            SELECT 
                CASE 
                    WHEN data_quality_score >= 80 THEN 'High Quality'
                    WHEN data_quality_score >= 50 THEN 'Medium Quality'
                    ELSE 'Low Quality'
                END as quality_tier,
                COUNT(*) as count,
                AVG(data_quality_score) as avg_score
            FROM users_transformed
            GROUP BY quality_tier
            ORDER BY avg_score DESC
        """).show()
        
        print("✅ Raw User Data Transformation Job completed successfully!")
        
    except Exception as e:
        print(f"❌ Job failed with error: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
    job.commit() 