
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("View_Parquet_File")
    .getOrCreate()
)

# Read Parquet file
df = spark.read.parquet(
    "output/clean/cleaned_employees"
)

# Display all records
df.show(truncate=False)

# Display schema
df.printSchema()

# Display total records
print("Total Records:", df.count())

spark.stop()
