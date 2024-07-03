from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)

# Specify the database and table names
database_name = "glue_db"
table_name_csv = "customer_billing_csv"
table_name_gzip_csv = "customer_billing_csv_gz"

# Read the data using DynamicFrame from the Glue Data Catalog
dynamic_frame_csv = glueContext.create_dynamic_frame.from_catalog(
    database=database_name, 
    table_name=table_name_csv
)

dynamic_frame_gzip_csv = glueContext.create_dynamic_frame.from_catalog(
    database=database_name, 
    table_name=table_name_gzip_csv
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
