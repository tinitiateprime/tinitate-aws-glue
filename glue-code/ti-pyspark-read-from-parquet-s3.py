from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)

# Specify the S3 path to the Parquet and Snappy-compressed Parquet files
parquet_path = "s3://ti-author-data/customer-billing/parquet/"
snappy_parquet_path = "s3://ti-author-data/customer-billing/snappy-parquet/"

# Read the regular Parquet file using DynamicFrame
dynamic_frame_parquet = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [parquet_path]},
    format="parquet"
)

# Read the Snappy-compressed Parquet file using DynamicFrame
dynamic_frame_snappy_parquet = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [snappy_parquet_path]},
    format="parquet"
)

# Convert DynamicFrames to DataFrames to use Spark SQL functionalities
df_parquet = dynamic_frame_parquet.toDF()
df_snappy_parquet = dynamic_frame_snappy_parquet.toDF()

# Example processing (simple show)
print("Count of rows in regular Parquet:", df_parquet.show(5))
print("Count of rows in Snappy Parquet:", df_snappy_parquet.show(5))

# Initialize and commit the job
job.init("parquet-read-job", {})
job.commit()
