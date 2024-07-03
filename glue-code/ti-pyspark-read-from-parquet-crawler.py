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
table_name_pq = "customer_billing_parquet"
table_name_snappy_pq = "customer_billing_snappy_parquet"


# Read the data using DynamicFrame from the Glue Data Catalog
dynamic_frame_pq = glueContext.create_dynamic_frame.from_catalog(
    database=database_name, 
    table_name=table_name_pq
)

dynamic_frame_snappy_pq = glueContext.create_dynamic_frame.from_catalog(
    database=database_name, 
    table_name=table_name_snappy_pq
)

# Convert DynamicFrames to DataFrames to use Spark SQL functionalities
df_pq = dynamic_frame_pq.toDF()
df_snappy_pq = dynamic_frame_snappy_pq.toDF()

# Example processing (simple count)
print("Reading JSON Conetents:", df_pq.show(5))
print("Reading JSON ZIP Conetents:", df_snappy_pq.show(5))

# Initialize and commit the job
job.init("manual-job-name", {})
job.commit()
