from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

# Initialize Spark context and Glue context
sc = SparkContext()
sc.setLogLevel("INFO")
glueContext = GlueContext(sc)

# Create or get an existing Spark session
spark = glueContext.spark_session

# Define Athena catalog and database
database = "glue_db"
table_name = "products_csv"

# Load the table "products_csv" from the Glue Data Catalog into a DataFrame
df = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=table_name).toDF()

# Calculate the distinct count of the bucketing column
bucket_column = "categoryid"
distinct_count = df.select(bucket_column).distinct().count()

# Define the base path in S3 where bucketed data will be stored
base_path = "s3://ti-author-scripts/ti-author-glue-scripts/ti-glue-pyspark-scripts-outputs/ti-glue-pyspark-bucketting-output/"

# Set the number of buckets based on the distinct count
num_buckets = min(distinct_count, 5)  # Adjust maximum number as appropriate

# Writing DataFrame bucketed by 'category' column
df.write.format("parquet") \
    .bucketBy(num_buckets, bucket_column) \
    .mode("overwrite") \
    .saveAsTable("bucketed_products", path=base_path)

# Read the bucketed data back to verify
bucketed_df = spark.read.parquet(base_path)

# Show schema to verify bucketing and see some data
bucketed_df.printSchema()
bucketed_df.show()
