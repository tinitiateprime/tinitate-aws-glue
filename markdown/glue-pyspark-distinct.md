# Managing Duplicate Records with PySpark in AWS Glue
This document provides a detailed guide on using PySpark within AWS Glue to remove duplicate records from a dataset, specifically focusing on a "purchase" table stored in Athena. The script sets up the necessary Spark and Glue contexts, loads the data, performs a distinct operation to ensure uniqueness, and displays the results.

## Prerequisites

Ensure proper configuration of IAM roles and S3 buckets and run necessary crawleras outlined here:
* [IAM Prerequisites](IAM-prerequisites.md)
* [S3 Data Generation](s3-data-generation.md)
* [Crawler Setup Instructions](set-up-instructions.md)
  
##  PySpark Script - [pyspark-distinct](../glue-code/ti-pyspark-distinct.py)
- Input tables          : purchase
- Output files          : Displays distinct records in AWS CloudWatch logs.
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
* Objective: Loads the "purchase" table from the Athena database into a DataFrame for subsequent filtering operations.
* Implementation :
  ```python
  df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="purchase").toDF()
  ```
### 3. Obtaining Distinct Records:
* Objective: Removes duplicate entries from the DataFrame to ensure that each row is unique, which is crucial for accurate data analysis and reporting.
* Implementation :
  ```python
  distinct_df = df.distinct()
  ```
 
### 4. Displaying Results:
* Objective: Display the unique records in the console for immediate verification, useful for debugging and quick data checks.
* Implementation :
  ```python
   print("Distinct Records:")
   distinct_df.show()

  ```
### 5. Logging and Execution Verification:
* Objective: Log the completion of the distinct operation and confirm the successful display of data.
* Implementation :
  ```python
  logger = glueContext.get_logger()
  logger.info("Distinct records successfully displayed in the console.")
  ```
