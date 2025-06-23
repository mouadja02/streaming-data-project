import uuid
import json
from kafka import KafkaProducer
import time
import logging
import requests

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def get_data():
    """Fetches user data from the randomuser.me API."""
    try:
        res = requests.get("https://randomuser.me/api/")
        res.raise_for_status()  # Raise an exception for bad status codes
        res_json = res.json()
        return res_json['results'][0]
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

def stream_data(producer, topic_name):
    """Streams data to a Kafka topic for one minute."""
    start_time = time.time()
    while time.time() < start_time + 60:  # Stream for 60 seconds
        try:
            raw_data = get_data()
            if raw_data:
                formatted_data = format_data(raw_data)
                
                # The value should be JSON formatted and encoded to utf-8
                producer.send(topic_name, json.dumps(formatted_data).encode('utf-8'))
                logging.info(f"Sent data for {formatted_data['first_name']} {formatted_data['last_name']}")
            time.sleep(1) # Sleep for a second to avoid overwhelming the API
        except Exception as e:
            logging.error(f'An error occurred during streaming: {e}')
            continue

if __name__ == "__main__":
    # Note: When running this script from your local machine (WSL),
    # 'localhost:9092' will correctly point to the Kafka service
    # exposed by Docker.
    kafka_producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 2) # Specify Kafka API version
    )

    topic = 'users_created'
    logging.info(f"Streaming data to Kafka topic: {topic}")
    stream_data(kafka_producer, topic)
    logging.info("Finished streaming data.") 