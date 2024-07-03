from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import Row
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()  # Use getOrCreate to avoid creating multiple contexts unintentionally
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

# Coalesce the DataFrame to one partition for a single output file
df_single = df.coalesce(1)

# Specify the S3 path for output of NDJSON format
output_dir_ndjson = "s3://ti-author-scripts/ti-author-glue-scripts/ti-glue-pyspark-scripts-outputs/ti-pyspark-write-to-ndjson-outputs/ndjson/"
output_dir_ndjson_gz = "s3://ti-author-scripts/ti-author-glue-scripts/ti-glue-pyspark-scripts-outputs/ti-pyspark-write-to-ndjson-outputs/ndjson-gz/"

# Write the data to S3 as NDJSON using Spark's DataFrame writer
df_single.write.format("json").option("lineSep", "\n").save(output_dir_ndjson)

# Write the data to S3 as gzip-compressed NDJSON using Spark's DataFrame writer
df_single.write.format("json").option("compression", "gzip").option("lineSep", "\n").save(output_dir_ndjson_gz)

print("Successfully written NDJSON and gzip-compressed NDJSON to S3")

# Commit the job to indicate successful completion
job.commit()
