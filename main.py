from kafka_hdfs_streaming.pyspark_streaming_kafka import process_csv_to_hdfs
from rawlayer import transform_and_output_to_hdfs
from cleansed import transform_and_save_to_hive_hdfs
from curated import sales_analysis

def main():
    process_csv_to_hdfs()
    transform_and_output_to_hdfs()
    transform_and_save_to_hive_hdfs()
    sales_analysis()

if __name__ == "__main__":
    main()