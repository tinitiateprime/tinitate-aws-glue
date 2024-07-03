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
output_dir_json = "s3://ti-author-scripts/ti-author-glue-scripts/ti-glue-pyspark-scripts-outputs/ti-pyspark-write-to-json-outputs/json/"
output_dir_json_gz = "s3://ti-author-scripts/ti-author-glue-scripts/ti-glue-pyspark-scripts-outputs/ti-pyspark-write-to-json-outputs/json-gz/"

# Write the data to S3 as a single JSON file using dynalic dataframe in glue
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_df_single,
    connection_type="s3",
    connection_options={"path": output_dir_json},
    format="json"
)

# Dynamic Frames doesn't support writiing to compressed json file, so use pyspark dataframe intead.
# Write the data to S3 as gzip-compressed JSON using Spark's DataFrame writer
df_single.write.format("json").option("compression", "gzip").save(output_dir_json_gz)

print("Successfully written the data in S3")

# Commit the job to indicate successful completion
job.commit()

