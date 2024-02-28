from quixstreams import Application
from quixstreams.models.serializers.quix import JSONDeserializer, JSONSerializer
import os
from datetime import timedelta, datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create an Application that uses local Kafka
app = Application(
  broker_address=os.environ.get('BROKER_ADDRESS','localhost:9092'),
  consumer_group=consumer_group_name,
  auto_create_topics=True,
  auto_offset_reset='earliest'
)

# Override the app variable if the local development env var is set to false or is not present.
localdev = os.environ.get('localdev', "false")

if localdev == "false":
    # Create a Quix platform-specific application instead
    app = Application.Quix(consumer_group=consumer_group_name, auto_create_topics=True, auto_offset_reset='earliest')

input_topic = app.topic(os.environ["input"], value_deserializer=JSONDeserializer())
output_topic = app.topic(os.environ["output"], value_serializer=JSONSerializer())

target_field = os.environ["target_field"]
logger.info(f"Target field is: {target_field}")

sdf = app.dataframe(input_topic)
sdf = sdf.update(lambda value: logger.info(f"Input value received: {value}"))

def custom_ts_extractor(value):
    """
    Specifying a custom timestamp extractor to use the timestamp from the message payload 
    instead of Kafka timestamp.
    """
    # Convert to a datetime object
    dt_obj = datetime.strptime(value["time_recorded"], "%Y-%m-%dT%H:%M:%S.%f")

    # Convert to milliseconds since the Unix epoch
    milliseconds = int(dt_obj.timestamp() * 1000)
    value["timestamp"] = milliseconds
    logger.info(f"Value of new timestamp is: {value['timestamp']}")
    return value["timestamp"]

# Passing the timestamp extractor to the topic.

# The window functions will now use the extracted timestamp instead of the Kafka timestamp.
topic = app.topic("input-topic", timestamp_extractor=custom_ts_extractor)

sdf = (
    # Extract the relevant field from the record
    sdf.apply(lambda value: value[target_field])

    # Define a tumbling window of 1 minute
    .tumbling_window(timedelta(minutes=1))

    # Specify the "mean" aggregation function to apply to values of the data key
    .mean()

    # Emit results only when the 1 minute window has elapsed
    .final()
    #.current() #for debug purposes.
)

sdf = sdf.apply(
    lambda value: {
        "time": value["end"],
        f"{target_field}": value["value"], 
    }
)

# Produce the result to the output topic
sdf = sdf.to_topic(output_topic)
sdf = sdf.update(lambda value: logger.info(f"Produced value: {value}"))

if __name__ == "__main__":
    logger.info("Starting application")
    app.run(sdf)
