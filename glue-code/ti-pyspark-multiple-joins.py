from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark and Glue contexts
sc = SparkContext()
sc.setLogLevel("INFO")
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Load data from the AWS Glue catalog
products_df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="products_csv").toDF()
categories_df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="categories_csv").toDF()
dispatch_df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="dispatch").toDF()

# Perform joins
# Join products with categories
product_category_df = products_df.join(categories_df, products_df.categoryid == categories_df.categoryid, "inner")

# Join the result with dispatch
product_category_df = products_df.join(categories_df, products_df.categoryid == categories_df.categoryid, "inner")
final_df = product_category_df.join(dispatch_df, product_category_df.productid == dispatch_df.product_id, "inner")

# Display results in console for CloudWatch logging
print("Final DataFrame:")
final_df.show()

# Log the operation completion in CloudWatch logs
glueContext.get_logger().info("Join operation completed successfully. Results displayed in console.")
