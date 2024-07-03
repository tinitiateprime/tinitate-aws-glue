# Managing Data Transformations and Table Creation with PySpark in AWS Glue

This documentation guides you through the process of using PySpark within AWS Glue to perform a CTAS operation. The workflow focuses on transforming data from the "purchase" table stored in Athena, creating a new table in the AWS Glue Data Catalog, and storing the transformed data directly in Amazon S3 using the CTAS approach.

## Overview of CTAS
The CTAS operation in AWS Glue is a powerful feature that allows for creating a new table based on the result of a SQL query. This is particularly useful for transforming and summarizing data before saving it into a new, optimized format.

## Prerequisites

Ensure proper configuration of IAM roles and S3 buckets and run necessary crawleras outlined here:
* [IAM Prerequisites](IAM-prerequisites.md)
* [S3 Data Generation](s3-data-generation.md)
* [Crawler Setup Instructions](set-up-instructions.md)
  
##  PySpark Script - [pyspark-ctas](../glue-code/ti-pyspark-ctas.py)
- Input tables          : purchase
- Output                : New table in csv format stored in S3 bucket and in glue_db.
- Crawlers used         : purchase_crawler


## Main Operations
### 1. Initializing Spark and Glue Contexts:
* Objective: Establishes the necessary Spark and Glue contexts for data manipulation with logging set to INFO to control verbosity.
* Implementation:
  ```python
  from pyspark.context import SparkContext
  from awsglue.context import GlueContext
  sc = SparkContext()
  sc.setLogLevel("INFO")
  glueContext = GlueContext(sc)
  ```
### 2. Data Loading and Transformation:
* Objective: Load the "purchase" data from Athena, apply transformations, and prepare for storage.
* Implementation:
  ```python
  purchase_df = glueContext.create_dynamic_frame.from_catalog(
    database="glue_db", table_name="purchase"
  ).toDF()
  
  filtered_df = purchase_df.filter(purchase_df["quantity"] > 100)
  filtered_df.createOrReplaceTempView("ctas_purchase_table")
  ```

### 3. CTAS Query Execution:
* Objective: Execute a CTAS query to create a new table with transformed data in the AWS Glue Data Catalog.
* Implementation:
  ```python
  ctas_query = """
  CREATE TABLE glue_db.ctas_filtered_purchase
  USING parquet
  OPTIONS (
      path 's3://ti-author-scripts/ti-author-glue-scripts/ti-glue-pyspark-scripts-outputs/ti-pyspark-ctas-outputs/'
  )
  AS SELECT * FROM ctas_purchase_table
  """
  ```
### 4. Logging and Verification:
* Objective: Log the completion of operations and verify the new table creation.
* Implementation:
  ```python
  glueContext.get_logger().info("CTAS operation completed and new table 'ctas_filtered_purchase' created in the Glue Data Catalog.")
  #Free up resources and close the session after the job completion.
  sc.stop()
  ```


