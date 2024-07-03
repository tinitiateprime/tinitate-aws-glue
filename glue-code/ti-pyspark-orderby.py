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

# Order by 'count' descending and display the results
result_df_desc = result_df.orderBy("count", ascending=False)
print("Ordered Descending:")
result_df_desc.show()

# Order by 'count' ascending and display the results
result_df_asc = result_df.orderBy("count", ascending=True)
print("Ordered Ascending:")
result_df_asc.show()

# Log information in console
print("Data successfully displayed in both ascending and descending order.")
