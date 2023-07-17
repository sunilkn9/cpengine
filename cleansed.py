from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2
from pyspark.sql.types import StringType

def transform_and_save_to_hive_hdfs():
    # Create a SparkSession with Hive support
    spark = SparkSession.builder \
        .appName("DataFrame Transformations Example") \
        .enableHiveSupport() \
        .getOrCreate()

    # Read the JSON file into a DataFrame
    json_file = "hdfs://localhost:9000/raw_layer"
    df = spark.read.json(json_file)

    # Encrypt sensitive columns using hashing techniques
    hashed_df = df.withColumn('LastName', sha2(col('LastName'), 256)) \
                  .withColumn('PhoneNumber', sha2(col('PhoneNumber'), 256)) \
                  .withColumn('EmailAddress', sha2(col('EmailAddress'), 256)) \
                  .withColumn('Address', sha2(col('Address'), 256))

    # Change the datatypes of hashed columns to StringType
    hashed_df = hashed_df.withColumn('LastName', col('LastName').cast(StringType())) \
                         .withColumn('PhoneNumber', col('PhoneNumber').cast(StringType())) \
                         .withColumn('EmailAddress', col('EmailAddress').cast(StringType())) \
                         .withColumn('Address', col('Address').cast(StringType()))

    # Save the transformed DataFrame to Hive and HDFS as JSON format
    hive_table = "hashed_data"
    hashed_df.write.mode("overwrite").format("hive").saveAsTable(hive_table)
    hdfs_output_path = "hdfs://localhost:9000/cleansed"
    hashed_df.write.mode("overwrite").json(hdfs_output_path)

    # Stop the SparkSession
    spark.stop()

# Call the function to perform the transformation and save to Hive and HDFS
transform_and_save_to_hive_hdfs()
