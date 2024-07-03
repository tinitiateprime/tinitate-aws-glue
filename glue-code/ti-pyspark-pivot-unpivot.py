from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, array, col

# Initialize Spark context with log level
sc = SparkContext()
sc.setLogLevel("INFO")  # Setting log level for Spark context

glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define Athena catalog and database
catalog = "awsglue_data_catalog"
database = "glue_db"

# Load CSV data into a DataFrame (For local testing, ensure the CSV path is correct and accessible)
df = spark.read.option("header", "true").csv("path_to_emp_dept.csv")  # Local CSV path

# Define columns to pivot
months = ['January', 'February', 'March']

# Pivot the data
pivot_df = df.select(
    "employee_id", "employee_name", "department",
    *[col(month + "_salary").alias(month) for month in months]
)

# Display the pivoted DataFrame
print("Pivoted DataFrame:")
pivot_df.show(truncate=False)

# Unpivot the pivoted DataFrame
unpivot_df = pivot_df.selectExpr(
    "employee_id", "employee_name", "department",
    "stack(3, 'January', January, 'February', February, 'March', March) as (month, salary)"
)

# Display the unpivoted DataFrame
print("Unpivoted DataFrame:")
unpivot_df.show(truncate=False)

# Log information after displaying results
glueContext.get_logger().info("Pivoted and unpivoted DataFrames successfully displayed in the console.")
