# Executing Multiple Joins:
The script facilitates the integration of product, category, and dispatch information, providing a holistic view of the inventory and dispatch dynamics. This integrated data can be critical for analyses like inventory management, sales performance, and operational efficiency.

## PySpark Script - [pyspark-joins](../glue-code/ti-pyspark-multiple-joins.py)
- Input tables          : products_csv, categories_csv, dispatch 
- Output files          : csv, json and parquet files in S3 buckets.
- Crawlers used         : product_crawler, category_crawler, dispatch_crawler

## Main Operations
### 1. Context Initialization:
  - Objective: Establish necessary contexts for Spark and Glue operations and set appropriate log levels.
  - Implementation:
    ```python
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("INFO")
    glueContext = GlueContext(sc)
    ```
### 2. Data Loading and Preparation:
  - Objective: Load tables from Athena into Spark DataFrames and prepare them for joining.
  - Implementation:
    ```python
    products_df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="products_csv").toDF()
    categories_df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="categories_csv").toDF()
    dispatch_df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="dispatch").toDF()
    ```
### 3. Executing Joins:
  - Objective: Perform inner joins between products_csv and categories_csv on categoryid, and between the resulting DataFrame and dispatch on productid.
  - Implementation:
    ```python
    product_category_df = products_df.join(categories_df, products_df.categoryid == categories_df.categoryid, "inner")
    final_df = product_category_df.join(dispatch_df, product_category_df.productid == dispatch_df.product_id, "inner")
    ```
### 4. Output Formatting and Storage:
   - Objective: Display the resulting data in the console for real-time viewing and log the completion of operations in AWS CloudWatch.
   - Implementation:
     ```python
      print("Final DataFrame:")
      final_df.show()
      glueContext.get_logger().info("Join operation completed successfully. Results displayed in console.")
     ```
