from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)

# Specify the S3 path to the CSV and gzip-compressed CSV files
csv_path = "s3://ti-author-data/customer-billing/csv/"
gzip_csv_path = "s3://ti-author-data/customer-billing/csv-gz/"

# Read the regular CSV file using DynamicFrame
dynamic_frame_csv = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [csv_path]},
    format="csv",
    format_options={"withHeader": True}
)

# Read the gzip-compressed CSV file using DynamicFrame
dynamic_frame_gzip_csv = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [gzip_csv_path]},
    format="csv",
    format_options={"withHeader": True, "compression": "gzip"}
)

# Convert DynamicFrames to DataFrames to use Spark SQL functionalities
df_csv = dynamic_frame_csv.toDF()
df_gzip_csv = dynamic_frame_gzip_csv.toDF()

# Showing the Dataframe Contents
print("Reading CSV Contents:", df_csv.show(5))
print("Reading ZIP CSV Contents:", df_gzip_csv.show(5))

# Initialize and commit the job
job.init("manual-job-name", {})
job.commit()
