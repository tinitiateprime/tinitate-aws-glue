from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

# Initialize Spark context and Spark session
sc = SparkContext.getOrCreate()
sc.setLogLevel("INFO")  # Setting log level for Spark context
spark = SparkSession(sc)
glueContext = GlueContext(sc)

# Load tables from Athena into data frames
product_df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="products_csv").toDF()
category_df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="categories_csv").toDF()

# Select specific columns from the tables to avoid duplicate column names
product_selected_df = product_df.select("productid", "productname", "categoryid", "unit_price").withColumnRenamed("categoryid", "product_categoryid")
category_selected_df = category_df.select("categoryid", "categoryname")

# Show initial DataFrames
print("DATAFRAME: product_selected_df")
product_selected_df.show()

print("DATAFRAME: category_selected_df")
category_selected_df.show()

# Inner Join
print("DATAFRAME: Inner Join")
inner_join_df = product_selected_df.join(
    category_selected_df,
    product_selected_df["product_categoryid"] == category_selected_df["categoryid"],
    "inner"
)
inner_join_df.show()

# Left Outer Join
print("DATAFRAME: Left Outer Join")
left_outer_join_df = product_selected_df.join(
    category_selected_df,
    product_selected_df["product_categoryid"] == category_selected_df["categoryid"],
    "left_outer"
)
left_outer_join_df.show()

# Right Outer Join
print("DATAFRAME: Right Outer Join")
right_outer_join_df = product_selected_df.join(
    category_selected_df,
    product_selected_df["product_categoryid"] == category_selected_df["categoryid"],
    "right_outer"
)
right_outer_join_df.show()

# Full Outer Join
print("DATAFRAME: Full Outer Join")
full_outer_join_df = product_selected_df.join(
    category_selected_df,
    product_selected_df["product_categoryid"] == category_selected_df["categoryid"],
    "outer"
)
full_outer_join_df.show()

# Left Semi Join
print("DATAFRAME: Left Semi Join")
left_semi_join_df = product_selected_df.join(
    category_selected_df,
    product_selected_df["product_categoryid"] == category_selected_df["categoryid"],
    "left_semi"
)
left_semi_join_df.show()

# Left Anti Join
print("DATAFRAME: Left Anti Join")
left_anti_join_df = product_selected_df.join(
    category_selected_df,
    product_selected_df["product_categoryid"] == category_selected_df["categoryid"],
    "left_anti"
)
left_anti_join_df.show()
