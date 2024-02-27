from quixstreams import Application
from quixstreams.models.serializers.quix import JSONDeserializer
import os
from influxdb_client_3 import InfluxDBClient3
import ast
import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Application.Quix(consumer_group="influx-destinationv5",
                       auto_offset_reset="earliest")

input_topic = app.topic(os.environ["input"], value_deserializer=JSONDeserializer())

# Read the environment variable and convert it to a list
tag_dict = ast.literal_eval(os.environ.get('INFLUXDB_TAG_COLUMNS', "{}"))

# Read the environment variable for measurement name and convert it to a list
measurement_name = os.environ.get('INFLUXDB_MEASUREMENT_NAME', os.environ["input"])
data_key = os.environ.get('data_key', 'activiation')
                                           
influx3_client = InfluxDBClient3(token=os.environ["INFLUXDB_TOKEN"],
                         host=os.environ["INFLUXDB_HOST"],
                         org=os.environ["INFLUXDB_ORG"],
                         database=os.environ["INFLUXDB_DATABASE"])

def send_data_to_influx(message):
    logger.info(f"Processing message: {message}")
    try:
        quixtime = message['time']

        try:
            logger.info(f"Official message timestamp is {message.time}")
        except Exception as e:
            logger.info(f"Cannot get message timestamp because: {e}")

        # Using point dictionary structure
        points = {
                "measurement": measurement_name,
                "tags": tag_dict,
                "fields": {data_key: message[data_key]},
                "time": quixtime
                }

        influx3_client.write(record=points, write_precision="ms")
        
        print(f"{str(datetime.datetime.utcnow())}: Persisted {len(message)} rows.")
    except Exception as e:
        print(f"{str(datetime.datetime.utcnow())}: Write failed")
        print(e)

sdf = app.dataframe(input_topic)
sdf = sdf.update(send_data_to_influx)

if __name__ == "__main__":
    print("Starting application")
    app.run(sdf)