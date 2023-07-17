from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def transform_and_output_to_hdfs():
    # Create a SparkSession with Hive support
    spark = SparkSession.builder \
        .appName("DataFrame Transformations Example") \
        .enableHiveSupport() \
        .getOrCreate()

    # Read multiple JSON files into a DataFrame
    json_file = "hdfs://localhost:9000/kafka_output"
    df = spark.read.json(json_file + "/*.json")
    df.show()

    # Transformation: Join FirstName, LastName, and Prefix into a single column
    concat_udf = udf(lambda prefix, first_name, last_name: f"{prefix} {first_name} {last_name}", StringType())
    df_transformed = df.withColumn("FullName", concat_udf(col("Prefix"), col("FirstName"), col("LastName")))

    # Transformation: Categorize age as "Young" or "Old"
    df_age_categorized = df_transformed.withColumn("AgeCategory", when(col("BirthDate") > "2000-01-01", "Young").otherwise("Old"))

    # Drop FirstName, LastName, and Prefix columns
    df_dropped = df_age_categorized.drop("FirstName", "LastName", "Prefix")

    # Check for null values in the DataFrame
    df_with_null_check = df_dropped.withColumn("HasNull", isnull(col("FullName")).cast("boolean"))

    # Output the transformed DataFrame to HDFS in JSON format
    output_path = "hdfs://localhost:9000/rawlayer"
    df_with_null_check.write.mode("overwrite").json(output_path)

    # Stop the SparkSession
    spark.stop()

# Call the function to perform the transformation and output to HDFS
transform_and_output_to_hdfs()
