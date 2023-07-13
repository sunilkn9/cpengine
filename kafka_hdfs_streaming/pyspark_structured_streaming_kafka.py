import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from kafka import KafkaProducer
import os

os.environ['HADOOP_HOME'] = "C:\hadoop-3.3.0"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

kafka_topic = "kafkahdfs"
kafka_server = "localhost:9092"
csv_file = r"C:\Users\sunil.kn\Downloads\salesdataset\AdventureWorksCustomers-210509-235702.csv"
output_path = "hdfs://localhost:9000/product_data1"


def read_csv_file(spark, csv_file):
    # Read the CSV file into a DataFrame
    df = spark.read.format("csv").option("header", "true").load(csv_file)
    df.show()
    return df


def convert_to_json(df):
    # Convert DataFrame to JSON and collect as a list of dictionaries
    json_data = df.toJSON().map(lambda x: json.loads(x)).collect()
    return json_data


def send_to_kafka(json_data):
    # Create Kafka producer
    producer = KafkaProducer(bootstrap_servers=kafka_server)

    # Send JSON data to Kafka topic
    for data in json_data:
        message = json.dumps(data)
        producer.send(kafka_topic, value=message.encode("utf-8"))
        print("Sent message:", message)

    # Close the producer
    producer.close()


def read_from_kafka(spark):
    # Read from Kafka topic as a streaming DataFrame
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    return kafka_df


def process_streaming_data(kafka_df, schema):
    # Process the streaming DataFrame as needed
    processed_df = kafka_df.selectExpr("CAST(value AS STRING)").select(
        from_json(col("value"), schema).alias("data")).select("data.*")
    return processed_df


def write_to_hdfs(processed_df, output_path):
    # Write the processed DataFrame to HDFS as JSON files
    query = processed_df \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", output_path) \
        .option("checkpointLocation", "hdfs://localhost:9000/checkpoint") \
        .start()

    # Wait for the streaming query to finish
    query.awaitTermination()


if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Demo Application Started ...")

    # Create a SparkSession
    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .getOrCreate()

    # Step 1: Read the CSV file
    df = read_csv_file(spark, csv_file)

    # Step 2: Convert DataFrame to JSON
    json_data = convert_to_json(df)

    # Step 3: Send JSON data to Kafka
    send_to_kafka(json_data)

    # Step 4: Read from Kafka topic as a streaming DataFrame
    kafka_df = read_from_kafka(spark)

    # Step 5: Process the streaming DataFrame
    processed_df = process_streaming_data(kafka_df, df.schema)

    # Step 6: Write the processed DataFrame to HDFS
    write_to_hdfs(processed_df, output_path)

    # Stop the SparkSession
    spark.stop()
