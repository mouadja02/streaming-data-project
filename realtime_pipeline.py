import uuid
import json
import time
import logging
import requests
import multiprocessing
import os
from kafka import KafkaProducer, KafkaConsumer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s'
)


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


# --- Simple Kafka Consumer (Fallback) ---

def run_simple_consumer_process():
    """Simple Kafka consumer that reads all messages and saves them to a file."""
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
            # Save to JSON file
            os.makedirs("./output", exist_ok=True)
            output_file = "./output/users_data.json"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(messages, f, indent=2, ensure_ascii=False)
            
            logging.info(f"✅ Successfully saved {len(messages)} records to {output_file}")
            
            # Also save as CSV for easy viewing
            import pandas as pd
            df = pd.DataFrame(messages)
            csv_file = "./output/users_data.csv"
            df.to_csv(csv_file, index=False)
            logging.info(f"✅ Also saved as CSV: {csv_file}")
        else:
            logging.warning("No messages found in Kafka topic.")
            
    except Exception as e:
        logging.error(f"Simple Consumer encountered an error: {e}", exc_info=True)
        raise e


# --- Spark Consumer Logic (Alternative) ---

def run_spark_consumer_process():
    """Spark consumer with simplified configuration."""
    logging.info("Starting Spark Consumer...")
    spark_conn = None
    try:
        # Import Spark here to avoid issues if not available
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import from_json, col
        from pyspark.sql.types import StructType, StructField, StringType
        
        # Simplified Spark configuration
        spark_conn = SparkSession.builder \
            .appName('KafkaDataConsumer') \
            .master('local[2]') \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.bindAddress", "localhost") \
            .config("spark.driver.port", "0") \
            .config("spark.blockManager.port", "0") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .getOrCreate()
        
        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully.")

        # Read from Kafka
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
    print("Kafka Producer → Consumer → Local File Output")
    
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
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n🛑 Shutdown signal received. Terminating processes...")
    except Exception as e:
        logging.error(f"❌ Pipeline failed: {e}", exc_info=True)
        exit(1) 