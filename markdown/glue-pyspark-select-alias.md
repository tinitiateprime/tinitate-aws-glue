# Selecting and Aggregation with PySpark in AWS Glue

This document describes the process of selecting specific columns from a products table in the AWS Glue Data Catalog, applying aliases to these columns, and displaying the modified output using PySpark. The goal is to simplify data representation and enhance readability for further data processing tasks or analytical reporting.

## Objectives

- Column Selection: Extract only the necessary columns from a larger dataset to focus on relevant data.
- Column Aliasing: Apply user-friendly names to the selected columns to improve the clarity of output displays and reports.

## Prerequisites

Ensure proper configuration of IAM roles and S3 buckets and run necessary crawleras outlined here:
* [IAM Prerequisites](IAM-prerequisites.md)
* [S3 Data Generation](s3-data-generation.md)
* [Crawler Setup Instructions](set-up-instructions.md)
  
##  PySpark Script - [pyspark-select-alias.py](../glue-code/ti-pyspark-select.py)
- Input tables          : products_csv
- Output                : Cloudwatch logs
- Crawlers used         : product_crawler

## Main Operations
### 1. Initializing Spark and Glue Contexts:
* Objective: Set up necessary contexts for PySpark and AWS Glue operations, ensuring that informative logging is enabled.
* Implementation :
  ```python
  from pyspark.context import SparkContext
  from awsglue.context import GlueContext
  sc = SparkContext()
  sc.setLogLevel("INFO")
  glueContext = GlueContext(sc)
  ```
  
### 2. Data Loading:
* Objective: Load data from the AWS Glue Data Catalog into Spark DataFrames, preparing it for subsequent processing.
* Implementation:
  ```python
  products_df = glueContext.create_dynamic_frame.from_catalog(
    database="glue_db", 
    table_name="products_csv"
  ).toDF()
  ```
### 3. Selecting and Aliasing Columns:
* Objective: Select specific columns from the DataFrame and rename them using aliases to enhance data readability and prepare for downstream processing.
* Implementation:
    ```python
  from pyspark.sql.functions import col
  selected_columns_df = products_df.select(
      col("productname").alias("Product Name"), 
      col("unit_price").alias("Unit Price")
  )
  ```
  
### 4. Displaying Results:
* Objective: Show the final DataFrame in the console, displaying the aliased columns to verify the correct execution and result format.
* Implementation:
  ```python
  print("Selected Columns with Aliases from Products Table:")
  selected_columns_df.show()
  ```

