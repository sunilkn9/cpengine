from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def read_json_file(spark, file_path):
    # Read JSON file into a DataFrame
    df = spark.read.json(file_path)
    return df

def get_total_orders(df):
    # Getting how many orders were placed
    total_orders = df.count()
    return total_orders

def get_average_revenue_per_order(sales_df, product_df):
    # Get Average Revenue Per Order JOIN
    average_revenue_per_order = sales_df.join(product_df, sales_df.ProductKey == product_df.ProductKey) \
        .select('ProductPrice', 'OrderLineItem') \
        .select((sum('ProductPrice') / count('OrderLineItem')).alias('Avg_rev_per_order'))
    return average_revenue_per_order

def get_average_revenue_per_day(sales_df, product_df):
    # Get Average Revenue Per Day JOIN
    average_revenue_per_day = sales_df.join(product_df, sales_df.ProductKey == product_df.ProductKey) \
        .select('ProductPrice', 'OrderLineItem', 'OrderDate') \
        .groupBy('OrderDate') \
        .agg((sum('ProductPrice') / count('OrderLineItem')).alias('Avg_rev_per_DAY')).orderBy(col('OrderDate').asc())
    return average_revenue_per_day

def get_average_revenue_per_month(average_revenue_per_day):
    # Get Average Revenue Per Month
    average_revenue_per_month = average_revenue_per_day.select(month('OrderDate').alias('MONTHS'), 'Avg_rev_per_DAY') \
        .groupBy('MONTHS') \
        .agg(avg('Avg_rev_per_DAY').alias('Avg_rev_per_MONTH')).orderBy(col('MONTHS').asc())
    return average_revenue_per_month

def get_total_revenue_per_month_per_year(average_revenue_per_day):
    # Get Total Revenue Per Month Per Year
    total_revenue_per_month_per_year = average_revenue_per_day.select(year('OrderDate').alias('YEAR'), 'Avg_rev_per_DAY') \
        .groupBy('YEAR') \
        .agg(avg('Avg_rev_per_DAY').alias('Avg_rev_per_YEAR'))
    return total_revenue_per_month_per_year

def get_highest_priced_product(product_df):
    # Get Highest Priced Product
    window = Window.orderBy(product_df['ProductPrice'].desc())
    highest_priced_product = product_df.select('*', rank().over(window).alias('rnk')).filter(col('rnk') == 1)
    return highest_priced_product

def get_number_of_orders_by_order_date(sales_df):
    # Get Number of Orders By Order Date
    number_of_orders_by_order_date = sales_df.groupBy('OrderDate') \
        .count().withColumnRenamed('count', 'Total_Orders') \
        .orderBy(sales_df['OrderDate'].asc())
    return number_of_orders_by_order_date

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("JSONTransformation").getOrCreate()

    # Read the JSON files into DataFrames
    sales_path = "hdfs://localhost:9000/sales_df/part-00000-1d9adccf-48da-4985-a0b0-98308b3845a0-c000.json"
    product_path = "hdfs://localhost:9000/product_df/part-00000-41a57faa-2c20-4060-8b29-ede76cf7399c-c000.json"
    sales_df = read_json_file(spark, sales_path)
    product_df = read_json_file(spark, product_path)

    # Getting how many orders were placed
    total_orders = get_total_orders(sales_df)
    print("Total orders:", total_orders)

    # Get Average Revenue Per Order JOIN
    average_revenue_per_order = get_average_revenue_per_order(sales_df, product_df)
    average_revenue_per_order.show()

    # Get Average Revenue Per Day JOIN
    average_revenue_per_day = get_average_revenue_per_day(sales_df, product_df)
    average_revenue_per_day.show()

    # Get Average Revenue Per Month
    average_revenue_per_month = get_average_revenue_per_month(average_revenue_per_day)
    average_revenue_per_month.show()

    # Get Total Revenue Per Month Per Year
    total_revenue_per_month_per_year = get_total_revenue_per_month_per_year(average_revenue_per_day)
    total_revenue_per_month_per_year.show()

    # Get Highest Priced Product
    highest_priced_product = get_highest_priced_product(product_df)
    highest_priced_product.show()

    # Get Number of Orders By Order Date
    number_of_orders_by_order_date = get_number_of_orders_by_order_date(sales_df)
    number_of_orders_by_order_date.show()

    # Stop the SparkSession
    spark.stop()
