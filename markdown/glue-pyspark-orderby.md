# Aggregation and Sorting with PySpark in AWS Glue

 This document outlines the process of using PySpark in AWS Glue to perform aggregation on data sourced from AWS Glue Data Catalog, followed by sorting and saving the results in various formats to Amazon S3.

## Overview of Aggregation

Aggregation in PySpark involves summarizing data from multiple rows into a single result. Typical aggregation functions include count, sum, avg, etc. In this script, we use the count function to determine the number of occurrences of each combination of 'make' and 'model' from a dataset of electric vehicles.

## Prerequisites

Ensure proper configuration of IAM roles and S3 buckets and run necessary crawleras outlined here:
* [IAM Prerequisites](IAM-prerequisites.md)
* [S3 Data Generation](s3-data-generation.md)
* [Crawler Setup Instructions](set-up-instructions.md)

##  PySpark Script - [pyspark-orderby.py](../glue-code/ti-pyspark-orderby.py)
- Input tables          : electric_vehicle_population_data_csv
- Output                : cloudwatch logs
- Crawlers used         : electric_vechiles

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
  grouped_df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="electric_vehicle_population_data_csv").toDF()
  ```

### 3. Data Aggregation and Sorting:
* Objective: Group data by 'make' and 'model', count occurrences, and sort the results in both ascending and descending order based on count.
* Implementation:
  ```python
  result_df = grouped_df.groupBy("make", "model").agg(count("*").alias("count"))
  result_df_desc = result_df.orderBy("count", ascending=False)
  result_df_asc = result_df.orderBy("count", ascending=True)
  ```
  
### 4. Displaying Results:
* Objective: Display the sorted data in the console for real-time observation, useful for debugging and quick data checks.
* Implementation:
  ```python
  print("Ordered Descending:")
  result_df_desc.show()
  print("Ordered Ascending:")
  result_df_asc.show()
  ```
  
### 5. Logging and Verification:
* Objective: Log the completion of data processing, confirming that the results have been successfully displayed.
* Implementation:
  ```python
  print("Data successfully displayed in both ascending and descending order.")
  ```
