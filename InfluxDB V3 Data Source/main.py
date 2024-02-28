# Importing necessary libraries and modules
import os
import random
import json
import logging
import uuid
from time import sleep

from quixstreams import Application
from quixstreams.models.serializers.quix import JSONSerializer, SerializationContext
import influxdb_client_3 as InfluxDBClient3


# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create an Application that uses local Kafka
app = Application(
  broker_address=os.environ.get('BROKER_ADDRESS','localhost:9092'),
  consumer_group=consumer_group_name,
  auto_create_topics=True
)

# Override the app variable if the local development env var is set to false or is not present.
localdev = os.environ.get('localdev', "false")

if localdev == "false":
    # Create a Quix platform-specific application instead
    app = Application.Quix(consumer_group=consumer_group_name, auto_create_topics=True)

# Define a serializer for messages, using JSON Serializer for ease
serializer = JSONSerializer()

# Define the topic using the "output" environment variable
topic_name = os.environ["output"]
topic = app.topic(name=topic_name, value_serializer="json")

influxdb3_client = InfluxDBClient3.InfluxDBClient3(token=os.environ["INFLUXDB_TOKEN"],
                         host=os.environ["INFLUXDB_HOST"],
                         org=os.environ["INFLUXDB_ORG"],
                         database=os.environ["INFLUXDB_DATABASE"])

measurement_name = os.environ.get("INFLUXDB_MEASUREMENT_NAME", os.environ["output"])
interval = os.environ.get("task_interval", "5m")

# should the main loop run?
# Global variable to control the main loop's execution
run = True

# Helper function to convert time intervals (like 1h, 2m) into seconds for easier processing.
# This function is useful for determining the frequency of certain operations.
UNIT_SECONDS = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 604800,
    "y": 31536000,
}

def interval_to_seconds(interval: str) -> int:
    try:
        return int(interval[:-1]) * UNIT_SECONDS[interval[-1]]
    except ValueError as e:
        if "invalid literal" in str(e):
            raise ValueError(
                "interval format is {int}{unit} i.e. '10h'; "
                f"valid units: {list(UNIT_SECONDS.keys())}")
    except KeyError:
        raise ValueError(
            f"Unknown interval unit: {interval[-1]}; "
            f"valid units: {list(UNIT_SECONDS.keys())}")

interval_seconds = interval_to_seconds(interval)

# Function to fetch data from InfluxDB and send it to Quix
# It runs in a continuous loop, periodically fetching data based on the interval.
def get_data():
    # Run in a loop until the main thread is terminated
    while run:
        try:
            myquery = f'SELECT * FROM "{measurement_name}" WHERE time >= {interval}'
            print(f"sending query {myquery}")
            # Query InfluxDB 3.0 using influxql or sql
            table = influxdb3_client.query(
                                    query=myquery,
                                    mode="pandas",
                                    language="influxql")

            table = table.drop(columns=["iox::measurement"])
            table.rename(columns={'time': 'time_recorded'}, inplace=True)
            # If there are rows to write to the stream at this time
            if not table.empty:
                json_result = table.to_json(orient='records', date_format='iso')
                yield json_result
                print("query success")
            else:
                print("No new data to publish.")

            # Wait for the next interval
            sleep(interval_seconds)

        except Exception as e:
            print("query failed", flush=True)
            print(f"error: {e}", flush=True)
            sleep(1)

def main():
    """
    Read data from the Query and publish it to Kafka
    """

    # Create a pre-configured Producer object.
    # Producer is already setup to use Quix brokers.
    # It will also ensure that the topics exist before producing to them if
    # Application.Quix is initialized with "auto_create_topics=True".

    with app.get_producer() as producer:
        for res in get_data():
            # Parse the JSON string into a Python object
            records = json.loads(res)
            for index, obj in enumerate(records):
                print(obj)
                # Generate a unique message_key for each row
                message_key = obj['machineId']
                logger.info(f"Produced message with key:{message_key}, value:{obj}")

                serialized = topic.serialize(
                    key=message_key, value=obj, headers={"uuid": str(uuid.uuid4())}
                    )

                # publish the data to the topic
                producer.produce(
                    topic=topic.name,
                    headers=serialized.headers,
                    key=serialized.key,
                    value=serialized.value,
                )

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")
