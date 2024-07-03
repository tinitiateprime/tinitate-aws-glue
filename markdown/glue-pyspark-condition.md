# SQL-like Filtering with PySpark in AWS Glue
This document provides an overview of using PySpark within AWS Glue to apply SQL-like filtering conditions, specifically on a "purchase" dataset stored in Athena. The script initializes the necessary Spark and Glue contexts, loads data, and applies various conditions such as IN, NOT IN, Greater Than, Less Than, and Not Equal To to filter the data accordingly.

## Prerequisites

Ensure proper configuration of IAM roles and S3 buckets and run necessary crawleras outlined here:
* [IAM Prerequisites](IAM-prerequisites.md)
* [S3 Data Generation](s3-data-generation.md)
* [Crawler Setup Instructions](set-up-instructions.md)
  
##  PySpark Script - [pyspark-filtering](../glue-code/ti-pyspark-condition.py)
- Input tables          : purchase
- Output files          : Displays filtered records in Cloudwatch Logs
- Crawlers used         : purchase_crawler

## Main Operations

### 1. Initializing Spark and Glue Contexts:
* Objective: Establish the foundational Spark and Glue contexts necessary for data manipulation, with logging configured to INFO level to manage verbosity.
* Implementation:
  ```python
  from pyspark.context import SparkContext
  from awsglue.context import GlueContext
  sc = SparkContext()
  sc.setLogLevel("INFO")
  glueContext = GlueContext(sc)
  ```

### 2. Data Loading:
* Objective: Loads the "purchase" table from the Athena database into a DataFrame for subsequent filtering operations.
* Implementation:
  ```python
  df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="purchase").toDF()
  ```
### 3. Applying Filters:
* Objective: Apply various SQL-like filters to demonstrate data segmentation based on specified criteria.
* Filters Applied:
  -  IN Condition: Filters rows where product_supplier_id matches any of the specified values.
    ```python
    df_in = df.filter(df["product_supplier_id"].isin([150, 259, 21]))
    print("IN Condition Results:")
    df_in.show()
    ```
  - NOT IN Condition: Excludes rows where quantity matches any of the specified values.
    ```python
    df_not_in = df.filter(~df["quantity"].isin([295, 743, 67]))
    print("NOT IN Condition Results:")
    df_not_in.show()

    ```
  - Greater Than Condition: Selects rows where quantity is greater than 200.
    ```python
    df_gt = df.filter(df["quantity"] > 200)
    print("Greater Than Condition Results:")
    df_gt.show()
    ```
  - Less Than Condition: Selects rows where quantity is less than 200.
    ```python
    df_lt = df.filter(df["quantity"] < 200)
    print("Less Than Condition Results:")
    df_lt.show()
    ```
  - Not Equal To Condition: Filters out rows where quantity is not equal to 743.
    ```python
    df_ne = df.filter(df["quantity"] != 743)
    print("Not Equal To Condition Results:")
    df_ne.show()
    ```    
    
### 4. Output Display:
* Objective: Display the filtered results directly in the console for immediate inspection and verification.
* Implementation: All results are shown using the show() method in the console.


### 5. Logging and Verification:
* Objective: Log the completion of the filter operations and confirm the successful display of data.
* Implementation:
  ```python
    glueContext.get_logger().info("Filtered data successfully displayed in the console for all conditions.")
  ```  
