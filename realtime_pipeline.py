import uuid
import json
import time
import logging
import requests
import multiprocessing
import os
import boto3
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class SparkUserDataS3Processor:
    """Handles S3 operations for user data processing using Spark DataFrames"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        
        # S3 Configuration
        self.s3_config = {
            'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'bucket_name': os.getenv('S3_BUCKET_NAME'),
            'region': os.getenv('AWS_REGION', 'us-east-1')
        }
        
        # S3 folder structure for user data
        self.s3_folders = {
            'raw_users': 'users/raw/parquet/',
            'user_analytics': 'users/analytics/parquet/',
            'user_demographics': 'users/demographics/parquet/'
        }
        
        # Initialize S3 client if credentials are available
        if self.s3_config['aws_access_key_id'] and self.s3_config['aws_secret_access_key']:
            self.init_s3_client()
            self.s3_enabled = True
        else:
            logger.warning("⚠️ S3 credentials not found. S3 upload will be skipped.")
            self.s3_enabled = False

    def init_s3_client(self):
        """Initialize S3 client"""
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.s3_config['aws_access_key_id'],
                aws_secret_access_key=self.s3_config['aws_secret_access_key'],
                region_name=self.s3_config['region']
            )
            
            # Test S3 connection
            if self.s3_config['bucket_name']:
                self.s3_client.head_bucket(Bucket=self.s3_config['bucket_name'])
                logger.info("✅ S3 connection established successfully")
            else:
                logger.warning("⚠️ S3 bucket name not configured")
                self.s3_enabled = False
            
        except Exception as e:
            logger.error(f"❌ Error initializing S3 client: {e}")
            self.s3_enabled = False

    def write_spark_df_to_s3(self, df, folder: str, filename: str) -> str:
        """Write Spark DataFrame to S3 as Parquet"""
        if not self.s3_enabled:
            logger.info("S3 not enabled, skipping upload")
            return None
            
        try:
            # Clean bucket name (remove s3:// prefix if present)
            bucket_name = self.s3_config['bucket_name']
            if bucket_name.startswith('s3://'):
                bucket_name = bucket_name.replace('s3://', '').split('/')[0]
            
            # Clean folder path (remove leading/trailing slashes)
            folder = folder.strip('/')
            
            # Construct proper S3A path (required for Spark)
            s3_path = f"s3a://{bucket_name}/{folder}/{filename}"
            
            logger.info(f"📤 Writing Spark DataFrame to S3: {s3_path}")
            
            df.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(s3_path)
            
            logger.info(f"✅ Successfully wrote DataFrame to {s3_path}")
            return s3_path
            
        except Exception as e:
            logger.error(f"❌ Error writing DataFrame to S3: {e}")
            return None

    def process_user_data(self, user_df) -> dict:
        """Process user data and create analytics using Spark DataFrames"""
        try:
            if user_df.count() == 0:
                logger.warning("No user data to process")
                return {}
                
            current_time = datetime.now()
            timestamp_str = current_time.strftime('%Y%m%d_%H%M%S')
            
            record_count = user_df.count()
            logger.info(f"🎯 Processing {record_count} user records with Spark")
            
            # 1. Save raw user data as Parquet to S3
            raw_users_filename = f"users_raw_{timestamp_str}"
            s3_path_raw = self.write_spark_df_to_s3(user_df, self.s3_folders['raw_users'], raw_users_filename)
            
            # 2. Calculate user analytics using Spark
            analytics_results = self.calculate_user_analytics_spark(user_df, current_time)
            
            # 3. Calculate demographics using Spark
            demographics_results = self.calculate_user_demographics_spark(user_df, current_time)
            
            results = {
                'processed_users': record_count,
                'raw_data_s3_path': s3_path_raw,
                'analytics': analytics_results,
                'demographics': demographics_results,
                'timestamp': current_time.isoformat()
            }
            
            logger.info(f"✅ Spark user data processing completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"❌ Error processing user data with Spark: {e}")
            import traceback
            traceback.print_exc()
            return {}

    def calculate_user_analytics_spark(self, user_df, timestamp: datetime) -> dict:
        """Calculate user analytics using Spark SQL"""
        try:
            from pyspark.sql.functions import col, count, split, regexp_extract
            
            timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
            
            # Create temporary view for SQL queries
            user_df.createOrReplaceTempView("users")
            
            # 1. Gender distribution using Spark SQL
            gender_stats = self.spark.sql("""
                SELECT 
                    'gender_distribution' as metric_type,
                    CONCAT('gender_', gender) as metric_name,
                    COUNT(*) as metric_value,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM users), 2) as percentage,
                    '{}' as created_at
                FROM users 
                GROUP BY gender
            """.format(timestamp.isoformat()))
            
            # 2. Email domain distribution using Spark SQL
            email_domain_stats = self.spark.sql("""
                SELECT 
                    'email_domain_distribution' as metric_type,
                    CONCAT('domain_', SPLIT(email, '@')[1]) as metric_name,
                    COUNT(*) as metric_value,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM users), 2) as percentage,
                    '{}' as created_at
                FROM users 
                GROUP BY SPLIT(email, '@')[1]
                ORDER BY metric_value DESC
                LIMIT 10
            """.format(timestamp.isoformat()))
            
            # 3. General metrics using Spark SQL
            general_metrics = self.spark.sql("""
                SELECT metric_type, metric_name, metric_value, percentage, created_at FROM (
                    SELECT 
                        'general' as metric_type,
                        'total_users' as metric_name,
                        COUNT(*) as metric_value,
                        100.0 as percentage,
                        '{}' as created_at
                    FROM users
                    
                    UNION ALL
                    
                    SELECT 
                        'general' as metric_type,
                        'unique_usernames' as metric_name,
                        COUNT(DISTINCT username) as metric_value,
                        ROUND(COUNT(DISTINCT username) * 100.0 / COUNT(*), 2) as percentage,
                        '{}' as created_at
                    FROM users
                )
            """.format(timestamp.isoformat(), timestamp.isoformat()))
            
            # Combine all analytics
            analytics_df = gender_stats.union(email_domain_stats).union(general_metrics)
            
            # Save analytics to S3
            analytics_filename = f"user_analytics_{timestamp_str}"
            s3_path = self.write_spark_df_to_s3(analytics_df, self.s3_folders['user_analytics'], analytics_filename)
            
            # Collect summary statistics for return
            gender_summary = gender_stats.collect()
            domain_summary = email_domain_stats.limit(5).collect()
            
            return {
                'analytics_records': analytics_df.count(),
                's3_path': s3_path,
                'gender_distribution': {row.metric_name: int(row.metric_value) for row in gender_summary},
                'top_email_domains': {row.metric_name: int(row.metric_value) for row in domain_summary}
            }
            
        except Exception as e:
            logger.error(f"❌ Error calculating user analytics with Spark: {e}")
            return {}

    def calculate_user_demographics_spark(self, user_df, timestamp: datetime) -> dict:
        """Calculate user demographics using Spark SQL"""
        try:
            from pyspark.sql.functions import split, trim, regexp_extract
            
            timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
            
            # Create temporary view
            user_df.createOrReplaceTempView("users")
            
            # Extract country and state from address using Spark SQL
            # Assuming address format: "number street, city, state, country"
            demographics_prep = self.spark.sql("""
                SELECT *,
                    TRIM(SPLIT(address, ',')[3]) as country,
                    TRIM(SPLIT(address, ',')[2]) as state
                FROM users
            """)
            
            demographics_prep.createOrReplaceTempView("users_with_location")
            
            # 1. Country distribution
            country_stats = self.spark.sql("""
                SELECT 
                    'country_distribution' as demographic_type,
                    country as demographic_value,
                    COUNT(*) as user_count,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM users_with_location), 2) as percentage,
                    '{}' as created_at
                FROM users_with_location 
                WHERE country IS NOT NULL AND country != ''
                GROUP BY country
                ORDER BY user_count DESC
            """.format(timestamp.isoformat()))
            
            # 2. State distribution (top 10)
            state_stats = self.spark.sql("""
                SELECT 
                    'state_distribution' as demographic_type,
                    state as demographic_value,
                    COUNT(*) as user_count,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM users_with_location), 2) as percentage,
                    '{}' as created_at
                FROM users_with_location 
                WHERE state IS NOT NULL AND state != ''
                GROUP BY state
                ORDER BY user_count DESC
                LIMIT 10
            """.format(timestamp.isoformat()))
            
            # Combine demographics
            demographics_df = country_stats.union(state_stats)
            
            # Save demographics to S3
            demographics_filename = f"user_demographics_{timestamp_str}"
            s3_path = self.write_spark_df_to_s3(demographics_df, self.s3_folders['user_demographics'], demographics_filename)
            
            # Collect summary statistics
            country_summary = country_stats.collect()
            state_summary = state_stats.limit(5).collect()
            
            return {
                'demographics_records': demographics_df.count(),
                's3_path': s3_path,
                'country_distribution': {row.demographic_value: int(row.user_count) for row in country_summary},
                'top_states': {row.demographic_value: int(row.user_count) for row in state_summary}
            }
            
        except Exception as e:
            logger.error(f"❌ Error calculating user demographics with Spark: {e}")
            return {}


# --- Kafka Producer Logic ---

def get_data():
    """Fetches user data from the randomuser.me API."""
    try:
        res = requests.get("https://randomuser.me/api/")
        res.raise_for_status()
        return res.json()['results'][0]
    except requests.exceptions.RequestException as e:
        logging.error(f"Could not get data from API: {e}")
        return None

def format_data(res):
    """Formats the raw user data into a structured dictionary."""
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data

def run_producer_process():
    """This function runs as a separate process to produce data to Kafka."""
    logging.info("Starting Kafka Producer...")
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            api_version=(0, 10, 2)
        )
        topic_name = 'users_created'
        start_time = time.time()
        record_count = 0
        
        logging.info(f"Streaming data to '{topic_name}' for 60 seconds.")
        while time.time() < start_time + 60:
            raw_data = get_data()
            if raw_data:
                formatted_data = format_data(raw_data)
                producer.send(topic_name, json.dumps(formatted_data).encode('utf-8'))
                record_count += 1
                logging.info(f"Produced record {record_count}: {formatted_data['first_name']} {formatted_data['last_name']}")
            time.sleep(1)
        
        logging.info(f"Kafka Producer finished. Total records produced: {record_count}")
    
    except Exception as e:
        logging.error(f"Kafka Producer encountered an error: {e}", exc_info=True)
    finally:
        if producer:
            producer.flush()
            producer.close()
        logging.info("Kafka Producer finished.")


# --- Consumer Logic ---

def run_simple_consumer_process():
    """Simple Kafka consumer that reads all messages and saves them to files."""
    logging.info("Starting Simple Kafka Consumer...")
    try:
        consumer = KafkaConsumer(
            'users_created',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='simple_consumer_group',
            value_deserializer=lambda x: x.decode('utf-8')
        )
        
        messages = []
        logging.info("Reading all messages from Kafka topic 'users_created'...")
        
        # Read all available messages with timeout
        start_time = time.time()
        timeout = 30  # 30 seconds timeout
        
        for message in consumer:
            try:
                data = json.loads(message.value)
                messages.append(data)
                logging.info(f"Consumed record {len(messages)}: {data['first_name']} {data['last_name']}")
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse JSON: {e}")
                continue
            
            # Check if we've been reading for too long without new messages
            if time.time() - start_time > timeout:
                logging.info("Timeout reached, stopping consumer...")
                break
        
        consumer.close()
        
        if messages:
            # Save to local files
            os.makedirs("./output", exist_ok=True)
            
            # Save as JSON
            output_file = "./output/users_data.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(messages, f, indent=2, ensure_ascii=False)
            logging.info(f"✅ Successfully saved {len(messages)} records to {output_file}")
            
            # Save as CSV using basic Python (no pandas dependency)
            csv_file = "./output/users_data.csv"
            if messages:
                fieldnames = messages[0].keys()
                import csv
                with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(messages)
                logging.info(f"✅ Also saved as CSV: {csv_file}")
                
        else:
            logging.warning("No messages found in Kafka topic.")
            
    except Exception as e:
        logging.error(f"Simple Consumer encountered an error: {e}", exc_info=True)
        raise e


def run_spark_consumer_process():
    """Spark consumer with simplified configuration."""
    logging.info("Starting Spark Consumer...")
    spark_conn = None
    try:
        # Import Spark here to avoid issues if not available
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import from_json, col
        from pyspark.sql.types import StructType, StructField, StringType
        
        s3_config = {
            'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'bucket_name': os.getenv('S3_BUCKET_NAME'),
            'region': os.getenv('AWS_REGION', 'us-east-1')
        }

        spark_conn = SparkSession.builder \
            .appName("KafkaDataConsumer") \
            .master("local[2]") \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                    "org.apache.kafka:kafka-clients:2.8.1,"
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/ecommerce-streaming/spark-checkpoints") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.hadoop.fs.s3a.access.key", s3_config['aws_access_key_id'] or "") \
            .config("spark.hadoop.fs.s3a.secret.key", s3_config['aws_secret_access_key'] or "") \
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{s3_config['region']}.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.path.style.access", "false") \
            .getOrCreate()
        
        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully.")

        # Read from Kafka in batch mode
        logging.info("Reading data from Kafka topic 'users_created'...")
        
        kafka_df = spark_conn.read \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .option('endingOffsets', 'latest') \
            .option('failOnDataLoss', 'false') \
            .load()

        record_count = kafka_df.count()
        logging.info(f"Found {record_count} records in Kafka.")
        
        if record_count == 0:
            logging.warning("No data found in Kafka topic.")
            return

        # Define schema
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("address", StringType(), True),
            StructField("post_code", StringType(), True),
            StructField("email", StringType(), True),
            StructField("username", StringType(), True),
            StructField("dob", StringType(), True),
            StructField("registered_date", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("picture", StringType(), True)
        ])

        # Parse JSON data
        selection_df = kafka_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')).select("data.*")
        
        parsed_count = selection_df.count()
        logging.info(f"Successfully parsed {parsed_count} records.")
        
        if parsed_count > 0:
            # Show sample data
            logging.info("Sample data:")
            selection_df.show(5, truncate=False)
            
            # Save to local parquet file
            os.makedirs("./output", exist_ok=True)
            output_path = "./output/users_spark_data.parquet"
            
            selection_df.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            logging.info(f"✅ Successfully wrote {parsed_count} records to {output_path}")
            
            # Process with S3 using Spark DataFrames
            try:
                s3_processor = SparkUserDataS3Processor(spark_conn)
                s3_results = s3_processor.process_user_data(selection_df)
                if s3_results:
                    logging.info(f"✅ S3 processing completed with Spark: {s3_results}")
            except Exception as e:
                logging.warning(f"⚠️ S3 processing failed: {e}")
                
        else:
            logging.warning("No valid records after parsing.")

    except Exception as e:
        logging.error(f"Spark Consumer error: {e}", exc_info=True)
        raise e
    finally:
        if spark_conn:
            logging.info("Stopping Spark session...")
            spark_conn.stop()
        logging.info("Spark Consumer finished.")


def run_consumer_process():
    """Consumer process that tries Spark first, then falls back to simple consumer."""
    try:
        logging.info("Attempting to use Spark consumer...")
        run_spark_consumer_process()
    except Exception as spark_error:
        logging.warning(f"Spark consumer failed: {spark_error}")
        logging.info("Falling back to simple Kafka consumer...")
        run_simple_consumer_process()


if __name__ == "__main__":
    print("--- Starting Sequential Data Pipeline ---")
    print("Kafka Producer → Consumer → Local Files + S3 Upload (if configured)")
    
    try:
        # Step 1: Run Kafka Producer
        logging.info("=" * 50)
        logging.info("STEP 1: Starting Kafka Producer")
        logging.info("=" * 50)
        
        producer_proc = multiprocessing.Process(name='KafkaProducer', target=run_producer_process)
        producer_proc.start()
        producer_proc.join()
        
        if producer_proc.exitcode == 0:
            logging.info("✅ Kafka Producer completed successfully!")
        else:
            logging.error(f"❌ Kafka Producer failed with exit code: {producer_proc.exitcode}")
            exit(1)
        
        # Step 2: Wait for Kafka to settle
        logging.info("Waiting 10 seconds for Kafka to settle...")
        time.sleep(10)
        
        # Step 3: Run Consumer
        logging.info("=" * 50)
        logging.info("STEP 2: Starting Consumer")
        logging.info("=" * 50)
        
        consumer_proc = multiprocessing.Process(name='Consumer', target=run_consumer_process)
        consumer_proc.start()
        consumer_proc.join(timeout=120)  # 2 minute timeout
        
        if consumer_proc.is_alive():
            logging.error("❌ Consumer timed out. Terminating...")
            consumer_proc.terminate()
            consumer_proc.join()
            exit(1)
        elif consumer_proc.exitcode == 0:
            logging.info("✅ Consumer completed successfully!")
        else:
            logging.error(f"❌ Consumer failed with exit code: {consumer_proc.exitcode}")
            exit(1)
            
        print("=" * 60)
        print("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        print("Check the './output/' directory for your data files:")
        print("  - users_data.json (JSON format)")
        print("  - users_data.csv (CSV format)")
        print("  - users_spark_data.parquet (if Spark worked)")
        print("If S3 is configured, check your S3 bucket for:")
        print("  - users/raw/parquet/ (raw user data)")
        print("  - users/analytics/parquet/ (user analytics)")
        print("  - users/demographics/parquet/ (user demographics)")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n🛑 Shutdown signal received. Terminating processes...")
    except Exception as e:
        logging.error(f"❌ Pipeline failed: {e}", exc_info=True)
        exit(1) 