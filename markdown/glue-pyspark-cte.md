# Temporary Views and CTEs Queries 
This document outlines using PySpark within AWS Glue to create temporary views and execute SQL queries, focusing on data from the "purchase" table stored in Athena. 
The script sets up the necessary Spark and Glue contexts, loads data, creates a temporary view, and queries it.

## CTE (Common Table Expression)
- Scope: Temporary views in PySpark are session-scoped and will not be visible in different Spark sessions. Once the Spark session ends, these views are destroyed.
- Purpose: They are used to make DataFrame data queryable using SQL without permanently storing the data in a database. It allows for more complex SQL operations to be performed on DataFrame data.
- Usage: After creating a temporary view, you can perform SQL queries on the data as if it were a table in a relational database. The view exists as long as the Spark session is active.
- Creation: Temporary views in PySpark are created using the createOrReplaceTempView method on a DataFrame, which registers the DataFrame as a temporary view in the Spark SQL catalog.

## Prerequisites

Ensure proper configuration of IAM roles and S3 buckets and run necessary crawleras outlined here:
* [IAM Prerequisites](IAM-prerequisites.md)
* [S3 Data Generation](s3-data-generation.md)
* [Crawler Setup Instructions](set-up-instructions.md)
  
##  PySpark Script - [pyspark-cte](../glue-code/ti-pyspark-cte.py)
- Input tables          : purchase
- Output files          : cloudwatch logs
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
### 2. Data Loading:
* Objective: Loads the "purchase" table from the Athena database into a DataFrame.
* Implementation:
  ```python
  df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="purchase").toDF()

  ```
### 3. Creating a Temporary View:
* Objective: Creates a temporary view named "temp_table" which can be used in SQL queries, similar to how a CTE would be used.
* Implementation:
  ```python
  df.createOrReplaceTempView("temp_table")
  ```

### 4. Executing SQL Queries and Displaying Result:
* Objective: Uses the temporary view to perform SQL queries, simplifying access to and manipulation of the data.
* Implementation:
  ```python
  result_df = spark.sql("SELECT * FROM temp_table")
  print("SQL Query Results:")
  result_df.show(truncate=False)
  ```
### 5. Executing a CTE:
* Objective: Executes the cte using the 'with' statement.
* Implementation:
  ```python
  # Define and execute a CTE
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
  ```
### 6. Displayng Result and Logging for Execution Verification:
* Objective: Displays the cte result and logs the completion of SQL queries and confirm the successful display of data.
* Implementation:
  ```python
  result_df = spark.sql(cte_query)
  print("SQL Query Results:")
  result_df.show(truncate=False)
  glueContext.get_logger().info("CTE query executed and results successfully displayed.")
  ```
