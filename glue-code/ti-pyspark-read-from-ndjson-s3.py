from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.job import Job

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = SparkSession.builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()
job = Job(glueContext)

# Specify the S3 paths to the NDJSON and gzip-compressed NDJSON files
ndjson_path = "s3://ti-author-data/customer-billing/ndjson/"
ndjson_gz_path = "s3://ti-author-data/customer-billing/ndjson-gz/"

# Read the regular NDJSON file using Spark DataFrame
df_ndjson = spark.read.option("lineSep", "\n").json(ndjson_path)

# Read the gzip-compressed NDJSON file using Spark DataFrame
df_ndjson_gz = spark.read.option("lineSep", "\n").option("compression", "gzip").json(ndjson_gz_path)

# Show the first few rows of the DataFrames to verify correct loading
print("Regular NDJSON file preview:")
df_ndjson.show(5)
print("Gzip-compressed NDJSON file preview:")
df_ndjson_gz.show(5)

# Initialize and commit the job
job.init("ndjson-read-job", {})
job.commit()
