import pandas as pd
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()

# Read CSV file using pandas
df = pd.read_csv(r'C:\Users\sunil.kn\Downloads\salesdataset\AdventureWorksReturns-210509-235702.csv')

# Convert pandas DataFrame to PySpark DataFrame
spark_df = spark.createDataFrame(df)

# Convert PySpark DataFrame to JSON
json_data = spark_df.toJSON().collect()

# Save JSON data to a file
with open('output.json', 'w') as f:
    for line in json_data:
        f.write(line + '\n')

print("CSV file converted to JSON successfully!")
