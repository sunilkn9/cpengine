from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def sales_analysis():
    # Create a SparkSession with Hive support
    spark = SparkSession.builder \
        .appName("Sales Analysis") \
        .enableHiveSupport() \
        .getOrCreate()

    # Read the product dataset
    product_path ="hdfs://localhost:9000/AdventureWorksProducts"
    product_df = spark.read.json(product_path+"/*.json") \
        .select("ProductKey", "ProductPrice", "ProductColor")

    # Read the sales dataset from local and select required columns
    sales_path = "hdfs://localhost:9000/AdventureWorksSales2015"
    sales_df = spark.read.json(sales_path+"/*.json") \
        .select("OrderNumber", "OrderDate", "OrderQuantity", "ProductKey")

    """Highest Priced Product:"""
    highest_priced_product = product_df.orderBy(desc("ProductPrice")).limit(1)
    highest_priced_product.show()

    """Number of Orders by Order Date:"""
    orders_by_date = sales_df.groupBy("OrderDate").agg(count("OrderNumber").alias("NumberofOrders"))
    orders_by_date.show()

    """Product Filtered by Color:"""
    product_df.filter(col("ProductColor") == "Red").show()

    """Number of Orders Placed:"""
    order_count1 = sales_df.groupBy("OrderNumber").count()
    order_count1.show()

    """Average Revenue Per Order:"""
    # Perform the join operation and calculate the average revenue per order
    joined_df = sales_df.join(product_df, "ProductKey")
    revenue_per_order_df = joined_df.groupBy("OrderNumber") \
        .agg(sum(col("ProductPrice")).alias("total_revenue"), count("OrderNumber").alias("order_count")) \
        .withColumn("average_revenue_per_order", col("total_revenue") / col("order_count"))
    revenue_per_order_df.show()

    """Average Revenue Per Day:"""
    average_revenue_per_day = joined_df.groupBy("OrderDate") \
        .agg((sum(col("ProductPrice") * col("OrderQuantity")) / count("OrderNumber"))
        .alias("average_revenue_per_day")).orderBy(col("OrderDate"))
    average_revenue_per_day.show()

    """Average Revenue Per Month:"""
    joined_df = joined_df.withColumn("OrderMonth", month(col("OrderDate"))) \
        .withColumn("OrderYear", year(col("OrderDate")))

    revenue_per_month_df = joined_df.groupBy("OrderMonth").agg(
        sum(col("ProductPrice")).alias("total_revenue"),
        count(col("OrderNumber")).alias("order_count")
    ).withColumn("average_revenue_per_month", col("total_revenue") / col("order_count"))
    revenue_per_month_df.show()

    """Total Revenue Per Month Per Year:"""
    revenue_per_month_per_year_df = joined_df.groupBy("OrderYear", "OrderMonth") \
        .agg(sum(col("ProductPrice")).alias("total_revenue"))
    revenue_per_month_per_year_df.show()

    # Save DataFrames to Hive and HDFS as JSON
    average_revenue_per_day.write.mode("overwrite").json("hdfs://localhost:9000/average_revenue_per_day")
    revenue_per_month_df.write.mode("overwrite").json("hdfs://localhost:9000/revenue_per_month_df")
    revenue_per_month_per_year_df.write.mode("overwrite").json("hdfs://localhost:9000/revenue_per_month_per_year_df")

    # Save DataFrames to Hive tables
    average_revenue_per_day.write.mode("overwrite").saveAsTable("average_revenue_per_day")
    revenue_per_month_df.write.mode("overwrite").saveAsTable("average_revenue_per_month")
    revenue_per_month_per_year_df.write.mode("overwrite").saveAsTable("revenue_per_month_per_year")

    # Stop the SparkSession
    spark.stop()

# Call the function to perform the sales analysis
sales_analysis()
