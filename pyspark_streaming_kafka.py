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
output_path = "hdfs://localhost:9000/kafka_output"


def process_csv_to_hdfs():
    spark = SparkSession.builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .getOrCreate()

    # Step 1: Read the CSV file
    def read_csv_file(spark, csv_file):
        df = spark.read.format("csv").option("header", "true").load(csv_file)
        df.show()
        return df

    # Step 2: Convert DataFrame to JSON
    def convert_to_json(df):
        json_data = df.toJSON().map(lambda x: json.loads(x)).collect()
        return json_data

    # Step 3: Send JSON data to Kafka
    def send_to_kafka(json_data):
        producer = KafkaProducer(bootstrap_servers=kafka_server)
        for data in json_data:
            message = json.dumps(data)
            producer.send(kafka_topic, value=message.encode("utf-8"))
            print("Sent message:", message)
        producer.close()

    # Step 4: Read from Kafka topic as a streaming DataFrame
    def read_from_kafka(spark):
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_server) \
            .option("subscribe", kafka_topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        return kafka_df

    # Step 5: Process the streaming DataFrame
    def process_streaming_data(kafka_df, schema):
        processed_df = kafka_df.selectExpr("CAST(value AS STRING)").select(
            from_json(col("value"), schema).alias("data")).select("data.*")
        return processed_df

    # Step 6: Write the processed DataFrame to HDFS
    def write_to_hdfs(processed_df, output_path):
        query = processed_df \
            .writeStream \
            .outputMode("append") \
            .format("json") \
            .option("path", output_path) \
            .option("checkpointLocation", "hdfs://localhost:9000/checkpoint") \
            .start()
        query.awaitTermination()  # Wait indefinitely

    try:
        # Execute the steps
        df = read_csv_file(spark, csv_file)
        json_data = convert_to_json(df)
        send_to_kafka(json_data)

        # Stop the SparkSession after sending data to Kafka
        spark.stop()
    except KeyboardInterrupt:
        print("Keyboard interrupt received. Stopping the program...")
        spark.stop()


if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Demo Application Started ...")
    process_csv_to_hdfs()
