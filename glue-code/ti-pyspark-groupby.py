from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Initialize Spark context and set the log level to INFO to capture informational messages during execution
sc = SparkContext.getOrCreate()
sc.setLogLevel("INFO")

# Create a GlueContext and retrieve the associated Spark session
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Setup logging
logger = glueContext.get_logger()
logger.info("Using Logger: Starting script execution")

# Define Athena catalog and database
catalog = "awsglue_data_catalog"
database = "glue_db"

# Load data from Athena into a DataFrame using Glue's DynamicFrame
electric_vehicles_df = glueContext.create_dynamic_frame.from_catalog(database=database, table_name="electric_vehicle_population_data_csv").toDF()

# Group by 'make' and 'model' columns, and count the occurrences
result_df = electric_vehicles_df.groupBy("make", "model").agg(count("*").alias("count"))

# Display the aggregated results in the console
print("Aggregated Results:")
result_df.show()

# Log that results are displayed
logger.info("Aggregated data displayed in console successfully.")

# Final log to indicate successful completion of the script
logger.info("Script execution completed successfully.")
