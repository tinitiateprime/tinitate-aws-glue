# Set Operations with PySpark in AWS Glue
This document provides a comprehensive guide on using PySpark within AWS Glue to perform set operations such as union, union all, and intersect and except focusing on data from "product" and "product_un_in" tables stored in Athena. The script sets up the necessary Spark and Glue contexts, loads the data, applies set operations to combine and compare datasets, and displays the results.

## Prerequisites

Ensure proper configuration of IAM roles and S3 buckets and run necessary crawleras outlined here:
* [IAM Prerequisites](IAM-prerequisites.md)
* [S3 Data Generation](s3-data-generation.md)
* [Crawler Setup Instructions](set-up-instructions.md)
  
##  PySpark Script - [pyspark-set-operations](../glue-code/ti-pyspark-union-unionall-intersect.py)
- Input tables          : products_csv, product_un_in
- Output files          : cloudwatch logs
- Crawlers used         : products_crawler

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

### 2. Data Loading:
* Objective: Loads the "product" and "product_un_in" tables from the Athena database into DataFrames to prepare them for set operations.
* Implementation:
  ```python
  df1 = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="product").toDF()
  df2 = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="product_un_in").toDF()

  ```
### 3. Applying Set Operations and Displaying Results:
* Union Operation: Combines rows from two datasets while removing duplicates.
  ```python
  union_df = df1.union(df2).distinct()
  print("Union Results:")
  union_df.show()
  ```
* Union All Operation: Combines all rows from two datasets, including duplicates (Note: In newer versions of PySpark, use union() to include duplicates).
  ```python
  union_all_df = df1.union(df2)
  print("Union All Results:")
  union_all_df.show()

  ```
* Intersect Operation: Retrieves only the common rows between two datasets.
  ```python
  # Intersect operation
  intersect_df = df1.intersect(df2)
  print("Intersect Results:")
  intersect_df.show()
  ```
* Except Operation: Retrieves records from the first DataFrame that do not exist in the second DataFrame.
  ```python
  except_df = df1.exceptAll(df2)
  print("Except Operation Results:")
  except_df.show()
  ```

### 5. Logging and Execution Verification:
* Objective: Log the completion of the set operations and confirm the successful display of data.
* Implementation:
  ```python
  glueContext.get_logger().info("Set operations successfully displayed in the console.")
  ```
