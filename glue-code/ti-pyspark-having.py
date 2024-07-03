from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Initialize Spark context with log level
sc = SparkContext()
sc.setLogLevel("INFO")  # Setting log level for Spark context

glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define Athena catalog and database
catalog = "awsglue_data_catalog"
database = "glue_db"

# Load tables from Athena into data frames
grouped_df = glueContext.create_dynamic_frame.from_catalog(database=database, table_name="electric_vehicle_population_data_csv").toDF()

# Group by electric vehicle column and count the occurrences
result_df = grouped_df.groupBy("make", "model").agg(count("*").alias("count"))

# Apply filter similar to HAVING clause
result_df_filtered = result_df.filter(result_df["count"] > 1000)

# Display the filtered DataFrame in the console
print("Filtered Results:")
result_df_filtered.show()

# Log information after displaying in the console
glueContext.get_logger().info("Filtered data successfully displayed in the console.")
