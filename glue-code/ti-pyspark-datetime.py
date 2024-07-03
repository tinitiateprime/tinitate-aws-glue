from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import to_timestamp, from_utc_timestamp, date_add, date_sub, year, month, dayofmonth, datediff, current_date, expr

# Initialize Spark context with log level
sc = SparkContext()
sc.setLogLevel("INFO")

glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define Athena catalog and database
catalog = "awsglue_data_catalog"
database = "glue_db"

# Load tables from Athena into data frames
df = glueContext.create_dynamic_frame.from_catalog(database=database, table_name="dispatch").toDF()

# Convert 'dispatch_date' to TimestampType
df_prepared = df.withColumn("parsed_date", to_timestamp(df["dispatch_date"], "dd-MMM-yyyy HH:mm:ss"))
df_prepared.show()

# Add and subtract days
df_add_days = df_prepared.withColumn("date_plus_10_days", date_add("parsed_date", 10))
df_sub_days = df_add_days.withColumn("date_minus_10_days", date_sub("parsed_date", 10))
df_sub_days.show()

# Extract year, month, and day from date
df_extracted = df_sub_days.withColumn("year", year("parsed_date"))
df_extracted = df_extracted.withColumn("month", month("parsed_date"))
df_extracted = df_extracted.withColumn("day", dayofmonth("parsed_date"))
df_extracted.show()

# Calculate the difference in years from the dispatch date to now
df_date_diff = df_extracted.withColumn("years_from_now", expr("floor(datediff(current_date(), parsed_date)/365)"))
df_date_diff.show()

# Log and print messages for each significant operation
print("Initial data loaded and date parsed.")
print("Added and subtracted 10 days from the original date.")
print("Extracted year, month, and day components from the date.")
print("Calculated the number of years from the current date to each dispatch date.")

# Optionally, you can also log using the GlueContext logger
glueContext.get_logger().info("Data processing completed.")
