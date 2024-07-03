from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession, functions as F # Import for built in pseudo functions in spark 
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import lit, current_date, current_timestamp, expr # Import for custom pseudo columns in spark

# Initialize Spark and Glue contexts
sc = SparkContext()
sc.setLogLevel("INFO")
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Sample data with two columns
data = [("Alice", 28), ("Bob", 34)]
schema = ["Name", "Age"]

# Create DataFrame
test_df = spark.createDataFrame(data, schema=schema)
print("Dataframe Created:")
test_df.show()

# Add a primary key column using inbuilt pseudo function 'monotonically_increasing_id()'.
test_df = test_df.withColumn("Primary_Key", F.monotonically_increasing_id())
print("Dataframe after add PK:")
test_df.show()

# Add custom columns with fixed values and current timestamps
test_df = test_df.withColumn("Custom_String", lit("Employee").cast(StringType())) \
                 .withColumn("Custom_Int", lit(100).cast(IntegerType())) \
                 .withColumn("Custom_Float", lit(123.456).cast("float")) \
                 .withColumn("Custom_Date", current_date()) \
                 .withColumn("Custom_DateTime", current_timestamp()) \
                 .withColumn("Custom_TimeZone", expr("from_utc_timestamp(current_timestamp(), 'America/New_York')"))

print("Final Dataframe:")
test_df.show(truncate=False)


