# Import necessary libraries for Spark and AWS Glue functionalities
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

# Initialize Spark context with log level
sc = SparkContext()
sc.setLogLevel("INFO")  # Setting log level for Spark context

glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define Athena catalog and database
catalog = "awsglue_data_catalog"
database = "glue_db"

# Load tables from Athena into data frames
df = glueContext.create_dynamic_frame.from_catalog(database=database, table_name="purchase").toDF()

# Create a temporary view of the DataFrame
df.createOrReplaceTempView("purchase_table")

# Define a CTE within a SQL query
cte_query = """
WITH SupplierTransactions AS (
    SELECT product_supplier_id, quantity, invoice_price, purchase_tnxdate
    FROM purchase_table
    WHERE purchase_tnxdate >= '2022-01-01'
)
SELECT product_supplier_id,
       SUM(quantity) AS total_quantity,
       SUM(quantity * invoice_price) AS total_spent
FROM SupplierTransactions
GROUP BY product_supplier_id
ORDER BY total_spent DESC
"""

# Use the CTE to execute the query
result_df = spark.sql(cte_query)

# Display the resulting DataFrame
print("Query Result:")
result_df.show(truncate=False)

# Log information after displaying the results
glueContext.get_logger().info("CTE query executed and results displayed successfully.")
