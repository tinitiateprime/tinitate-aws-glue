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
df1 = glueContext.create_dynamic_frame.from_catalog(database=database, table_name="products_csv").toDF()
df2 = glueContext.create_dynamic_frame.from_catalog(database=database, table_name="product_un_in").toDF()

# Union operation (remove duplicates)
union_df = df1.union(df2).distinct()
print("Union (Distinct) Results:")
union_df.show()

# Union all operation (include duplicates)
union_all_df = df1.union(df2)  # In newer versions of PySpark, use union() instead of unionAll()
print("Union All Results:")
union_all_df.show()

# Intersect operation
intersect_df = df1.intersect(df2)
print("Intersect Results:")
intersect_df.show()

# Perform Except operation
except_df = df1.exceptAll(df2)
print("Except Operation Results:")
except_df.show()

# Log information after displaying results
glueContext.get_logger().info("Results successfully displayed in the console for union, union all, and intersect operations.")
