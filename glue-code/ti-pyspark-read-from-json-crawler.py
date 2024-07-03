from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)

# Specify the database and table name in the Glue Data Catalog
database_name = "glue_db"
table_name_json = "customer_billing_json"
table_name_gzip_json = "customer_billing_gz_json"


# Read the data using DynamicFrame from the Glue Data Catalog
dynamic_frame_json = glueContext.create_dynamic_frame.from_catalog(
    database=database_name, 
    table_name=table_name_json
)

dynamic_frame_gzip_json = glueContext.create_dynamic_frame.from_catalog(
    database=database_name, 
    table_name=table_name_gzip_json
)

# Convert DynamicFrames to DataFrames to use Spark SQL functionalities
df_json = dynamic_frame_json.toDF()
df_gzip_json = dynamic_frame_gzip_json.toDF()

# Example processing (simple count)
print("Reading JSON Conetents:", df_json.show())
print("Reading JSON ZIP Conetents:", df_gzip_json.show())

# Initialize and commit the job
job.init("manual-job-name", {})
job.commit()
