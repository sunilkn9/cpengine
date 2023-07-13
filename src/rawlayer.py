from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import IntegerType, DoubleType

def read_json_file(spark, file_path):
    # Read JSON file into a DataFrame
    df = spark.read.json(file_path)
    return df

def check_null_values(df):
    # Check for null values in DataFrame
    print("Null values:")
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        print(f"Null count in {column}: {null_count}")

def change_column_data_types(df, column_names, new_types):
    # Change column data types in DataFrame
    for column, new_type in zip(column_names, new_types):
        df = df.withColumn(column, col(column).cast(new_type))
    return df

def drop_duplicates(df):
    # Drop duplicates based on all columns in DataFrame
    df = df.dropDuplicates()
    return df

if __name__ == "__main__":
    # Create a SparkSession with Hive support
    spark = SparkSession.builder.appName("JSONReader").enableHiveSupport().getOrCreate()

    # Read multiple JSON files from HDFS
    product_path = "hdfs://localhost:9000/AdventureWorksProducts/part-00000-1b40cbf7-7cc7-4cdf-9a59-554475ca124a-c000.json"
    sales_path = "hdfs://localhost:9000/sales_data/part-00000-69c4392f-dc09-409f-a94e-37f611032bfa-c000.json"
    product_df = read_json_file(spark, product_path)
    sales_df = read_json_file(spark, sales_path)

    # Print the original schema of product_df
    print("Schema of product_df:")
    product_df.printSchema()

    # Check for null values in product_df
    check_null_values(product_df)

    # Change column data types in product_df
    product_df = change_column_data_types(product_df, ["ProductKey", "ProductSubcategoryKey", "ProductCost", "ProductPrice"],
                                          [IntegerType(), IntegerType(), DoubleType(), DoubleType()])

    # Drop duplicates based on all columns in product_df
    product_df = drop_duplicates(product_df)

    # Print the resulting schema of product_df
    print("\nSchema of product_df after dropping duplicates:")
    product_df.printSchema()

    # Print the first few rows of product_df after dropping duplicates
    print("\nFirst few rows of product_df after dropping duplicates:")
    product_df.show(5)

    # Print the original schema of sales_df
    print("Schema of sales_df:")
    sales_df.printSchema()

    # Check for null values in sales_df
    check_null_values(sales_df)

    # Change data types of 'OrderDate' and 'StockDate' columns in sales_df
    sales_df = change_column_data_types(sales_df, ["OrderDate", "StockDate"], [TimestampType(), TimestampType()])

    # Drop duplicates based on all columns in sales_df
    sales_df = drop_duplicates(sales_df)

    # Print the resulting schema of sales_df
    print("\nSchema of sales_df after dropping duplicates:")
    sales_df.printSchema()

    # Print the first few rows of sales_df after dropping duplicates
    print("\nFirst few rows of sales_df after dropping duplicates:")
    sales_df.show(5)

    # Save product_df and sales_df to HDFS
    product_df.write.mode("overwrite").json("hdfs://localhost:9000/product_df")
    sales_df.write.mode("overwrite").json("hdfs://localhost:9000/sales_df")

    # Save sales_df to Hive
    sales_df.write.mode("overwrite").saveAsTable("sales_df")
    product_df.write.mode("overwrite").saveAsTable("product_df")

    # Stop the SparkSession
    spark.stop()
