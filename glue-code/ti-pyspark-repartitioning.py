from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

# Initialize Spark context and Glue context
sc = SparkContext()
sc.setLogLevel("INFO")
glueContext = GlueContext(sc)

# Create or get an existing Spark session
spark = glueContext.spark_session

# Load your data into a DataFrame from the AWS Glue Data Catalog
df = glueContext.create_dynamic_frame.from_catalog(
    # Define Athena catalog and database
    database = "glue_db",
    table_name="products_csv"
).toDF()

# Check the current number of partitions
current_partitions = df.rdd.getNumPartitions()
print("Current number of partitions:", current_partitions)

# Define the base path in S3 where you want to save the files
base_path = "s3://ti-author-scripts/ti-author-glue-scripts/ti-glue-pyspark-scripts-outputs/ti-glue-pyspark-repartitioning-outputs/"

# Function to write data in specified formats
def write_data(df, path_suffix, repartition=None):
    if repartition is not None:
        df = df.repartition(repartition)
    
    # Write to Parquet
    df.write.format("parquet").mode("overwrite").save(base_path + path_suffix + "parquet/")

    # Write to JSON
    df.write.format("json").mode("overwrite").save(base_path + path_suffix + "json/")

    # Write to CSV
    df.write.format("csv").option("header", "true").mode("overwrite").save(base_path + path_suffix + "csv/")

# Write the data with the current number of partitions
write_data(df, "current_partitions/")

# Increase the number of partitions by one
new_partitions = current_partitions + 1
print("Increased number of partitions:", new_partitions)

# Write the data with the increased number of partitions
write_data(df, "increased_partitions/", repartition=new_partitions)
