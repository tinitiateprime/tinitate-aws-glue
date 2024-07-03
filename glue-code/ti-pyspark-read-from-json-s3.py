from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# Initialize Spark Context and Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession.builder.master("local").appName("GlueJob").getOrCreate()  # Ensure SparkSession is created

job = Job(glueContext)

# Specify the S3 path to the JSON and gzip-compressed JSON files
json_path = "s3://ti-author-data/customer-billing/json/"
gzip_json_path = "s3://ti-author-data/customer-billing/json-gz/"

# Read the regular JSON file using DynamicFrame
dynamic_frame_json = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [json_path]},
    format="json"
)

# Read the gzip-compressed JSON file using Spark DataFrame as DynamicFrame doesn't support reads from compressed json file.
df_gzip_json = spark.read.option("compression", "gzip").json(gzip_json_path)

# Convert DynamicFrame to DataFrame to use Spark SQL functionalities
df_json = dynamic_frame_json.toDF()

# Example processing (simple count)
print("Regular JSON file preview:")
df_json.show(5)
print("Compressed JSON file preview:")
df_gzip_json.show(5)

# Initialize and commit the job
job.init("manual-job-name", {})
job.commit()
