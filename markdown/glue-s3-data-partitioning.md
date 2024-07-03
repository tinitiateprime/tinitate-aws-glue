# Data Partitioning and Storage with PySpark in AWS Glue
This document outlines the process of loading, partitioning, and storing data using PySpark within AWS Glue. The script demonstrates how to manage and write partitioned data to Amazon S3 in different formats, leveraging partitioning capabilities to enhance data retrieval and processing efficiency.

## Overview:
1. Partitioning: Organizing data in a structured format within S3 to enhance query performance and data management.
2. Data Formats: Writing data in multiple formats (Parquet, JSON, CSV) to support various use cases and systems.
   
## Prerequisites
Ensure proper configuration of IAM roles and S3 buckets and run necessary crawleras outlined here:
* [IAM Prerequisites](IAM-prerequisites.md)
* [S3 Data Generation](s3-data-generation.md)
* [Crawler Setup Instructions](set-up-instructions.md)
  
##  PySpark Script - [pyspark-repartitioning.py](../glue-code/ti-pyspark-repartitioning.py)
* Input Table: electric_vehicle_population_data_csv
* Output Formats: CSV, JSON, and Parquet files in S3 buckets.
* Crawlers Used : eletricc_vehicles

## Main Operations
### 1. Initializing Spark and Glue Contexts:
  * Objective: Configures the Spark and Glue contexts to ensure proper execution of operations with informative logging.
  * Implementation:
    ```python
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    sc = SparkContext()
    sc.setLogLevel("INFO")
    glueContext = GlueContext(sc)
    ```
### 2. Data Loading:
  * Objective: Load the purchase table from Athena into a DataFrame, preparing it for transformation.
  * Implementation:
    ```python
    df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="electric_vehicle_population_data_csv").toDF()

    ```
### 3. Data Partitioning:
  * Objective: Partition data based on specific columns to optimize storage and query performance.
  * Explanation: Partitioning is a tool that allows you to control what data is stored and where as you write it. By encoding a column as a folder when writing to a partitioned directory, you can dramatically improve read performance. This approach allows you to skip large amounts of irrelevant data when you read in the dataset later, focusing only on the data pertinent to your queries.
  * Implementation:
    ```python
    partition_columns = ["Model Year", "Make"]
    ```

### 4. Write Partitioned Data to S3:
  * Objective: Write the partitioned data to S3 in various formats, enabling efficient data storage and access.
  * Implementation:
    ```python
    df.write.partitionBy(*partition_columns).format("parquet").mode("overwrite").save(s3_base_path + "parquet/")
    df.write.partitionBy(*partition_columns).format("json").mode("overwrite").save(s3_base_path + "json/")
    df.write.partitionBy(*partition_columns).format("csv").option("header", "true").mode("overwrite").save(s3_base_path + "csv/")

    ```
  * Sample output for Partition By Column : "Model Year"
    <img width="928" alt="partition_1" src="https://github.com/sarutlaa/tinitiate-aws-glue/assets/141533429/fe59fb7c-75a1-4b6a-a84a-a4e2e2337d7d">
  * Sample output for Next Partition By Columns : "Make"
    <img width="934" alt="repartition_1" src="https://github.com/sarutlaa/tinitiate-aws-glue/assets/141533429/0bed9b85-7a29-4657-9d0c-081ab9ca895e">
    
### 5. Verify Data Structure:
  * Objective: Track the success and details of the data processing and writing operations.
  * Implementation:
    ```python
    df.printSchema()
    ```
