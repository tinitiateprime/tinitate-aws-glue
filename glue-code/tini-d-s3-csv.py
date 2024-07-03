from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

# Create Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read data from source S3 bucket
source_df = spark.read.format("csv").option("header", "true").load("s3://tini-d-gluebucket-001/Source-csv/source-csv.csv")

# Write data to destination S3 bucket with the same file name
target_path = "s3://tini-d-gluebucket-001/target-csv/"
source_df.write.format("csv").option("header", "true").mode("overwrite").save(target_path)
