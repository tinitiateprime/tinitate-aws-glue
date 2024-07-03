from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import Row
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)

# Sample data creation
data = [
    Row(name="John Doe", age=30, city="New York"),
    Row(name="Jane Smith", age=25, city="Los Angeles"),
    Row(name="Mike Johnson", age=35, city="Chicago")
]

# Convert sample data to DataFrame
df = sc.parallelize(data).toDF()

# Use coalesce to reduce the DataFrame to one partition
df_single = df.coalesce(1)

# Convert the coalesced DataFrame to DynamicFrame
dynamic_df_single = DynamicFrame.fromDF(df_single, glueContext, "dynamic_df_single")

# Specify the S3 path for output
output_dir_parquet = "s3://ti-author-scripts/ti-author-glue-scripts/ti-glue-pyspark-scripts-outputs/ti-pyspark-write-to-parquet-outputs/parquet/"
output_dir_parquet_snappy = "s3://ti-author-scripts/ti-author-glue-scripts/ti-glue-pyspark-scripts-outputs/ti-pyspark-write-to-parquet-outputs/parquet-snappy/"

# Write the data to S3 as a single Parquet file
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_df_single,
    connection_type="s3",
    connection_options={"path": output_dir_parquet},
    format="parquet",
    format_options={"compression": "uncompressed"}
)

# Write the data to S3 as a single Snappy-compressed Parquet file
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_df_single,
    connection_type="s3",
    connection_options={"path": output_dir_parquet_snappy},
    format="parquet",
    format_options={"compression": "snappy"}
)

print("Successfully written the data in S3")

# Commit the job to indicate successful completion
job.commit()
