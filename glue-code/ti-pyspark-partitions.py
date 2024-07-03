from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

# Initialize Spark context with log level
sc = SparkContext()
sc.setLogLevel("INFO")  # Setting log level for Spark context to INFO

glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define Athena catalog and database
catalog = "awsglue_data_catalog"
database = "glue_db"

# Load the table "electric_vehicles" from the Glue Data Catalog into a DataFrame
df = glueContext.create_dynamic_frame.from_catalog(database=database, table_name="electric_vehicles").toDF()

# Define the partitioning columns
partition_columns = ["model_year", "make"]

# Base path for S3 bucket where data will be stored
s3_base_path = "s3://ti-author-scripts/ti-author-glue-scripts/ti-pyspark-partitions.py"

# Write the DataFrame to S3 in Parquet format, partitioned by specified columns
df.write.partitionBy(*partition_columns).format("parquet").mode("overwrite").save(s3_base_path + "parquet/")

# Write the DataFrame to S3 in JSON format, partitioned by specified columns
df.write.partitionBy(*partition_columns).format("json").mode("overwrite").save(s3_base_path + "json/")

# Write the DataFrame to S3 in CSV format, partitioned by specified columns
df.write.partitionBy(*partition_columns).format("csv").mode("overwrite").option("header", "true").save(s3_base_path + "csv/")

# Print the schema of the DataFrame to verify the structure
df.printSchema()
