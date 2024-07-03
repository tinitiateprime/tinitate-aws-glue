from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

# Initialize Spark context with log level
sc = SparkContext()
sc.setLogLevel("INFO")  # Set log level to INFO to control the verbosity of log output

glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define Athena catalog and database
catalog = "awsglue_data_catalog"
database = "glue_db"

# Load tables from Athena into data frames
df = glueContext.create_dynamic_frame.from_catalog(database=database, table_name="purchase").toDF()

# IN condition
df_in = df.filter(df["product_supplier_id"].isin([150, 259, 21]))
print("Rows with product_supplier_id IN [150, 259, 21]:")
df_in.show(truncate=False)

# NOT IN condition
df_not_in = df.filter(~df["quantity"].isin([295, 743, 67]))
print("Rows with quantity NOT IN [295, 743, 67]:")
df_not_in.show(truncate=False)

# Greater than condition
df_gt = df.filter(df["quantity"] > 200)
print("Rows with quantity > 200:")
df_gt.show(truncate=False)

# Less than condition
df_lt = df.filter(df["quantity"] < 200)
print("Rows with quantity < 200:")
df_lt.show(truncate=False)

# Not equal to condition
df_ne = df.filter(df["quantity"] != 743)
print("Rows with quantity != 743:")
df_ne.show(truncate=False)

# Logging information to console instead of S3
glueContext.get_logger().info("Filtered data successfully displayed in the console for all conditions.")
