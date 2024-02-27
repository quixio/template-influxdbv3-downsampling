# Downsampling template

This basic template contains:

A real time data processing pipeline with these services:

 - Machine Data to InfluxDB- A script that generates synthetic machine data and writes it to InfluxDB (useful if you dont have your own data yet, or just want to work with test data first). It produces a reading every 250 milliseconds.
 - InfluxDB V3 Data Source - A service that queries for fresh data from InfluxDB at specific intervals. It's configured to look for the measurement produced by the previously-mentioned synthetic machine data generator. It writes the raw data to a Kafka topic called "raw-data".
 - Downsampler - A service that performs a 1-minute tumbling window operation on the data from InfluxDB and emits the mean of the "temperature" reading every minute. It writes the output to a "downsampled-data" Kafka topic.
 - InfluxDB V3 Data Sink - A service that reads from the "downsampled-data" topic and writes the downsample records as points back into InfluxDB.

We have also included a `docker-compose.yml` file so you can run the whole pipeline locally, including the message broker.
