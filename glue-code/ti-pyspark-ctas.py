from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

# Initialize Spark and Glue contexts
sc = SparkContext()
sc.setLogLevel("INFO")
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Load the 'purchase' table from the AWS Glue Data Catalog
purchase_df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="purchase").toDF()

print("Original Table:")
purchase_df.show()

# Filter purchases where the quantity is greater than a threshold, e.g., 100
filtered_df = purchase_df.filter(purchase_df["quantity"] > 100)

# Create or replace a temporary view to use in the SQL CTAS statement
filtered_df.createOrReplaceTempView("ctas_purchase_table")

# Define the CTAS query to create a new table in the AWS Glue Data Catalog
ctas_query = """
CREATE TABLE glue_db.ctas_filtered_purchase
USING parquet
OPTIONS (
    path 's3://ti-author-scripts/ti-author-glue-scripts/ti-glue-pyspark-scripts-outputs/ti-pyspark-ctas-outputs/'
)
AS SELECT * FROM ctas_purchase_table
"""

# Execute the CTAS operation
spark.sql(ctas_query)
print("CTAS operation executed. New table 'ctas_filtered_purchase' created in the Glue Data Catalog.")

# Log the completion of the CTAS operation
glueContext.get_logger().info("CTAS operation completed and new table 'ctas_filtered_purchase' created in the Glue Data Catalog.")

# Stop the Spark context to free up resources and close the session
sc.stop()
