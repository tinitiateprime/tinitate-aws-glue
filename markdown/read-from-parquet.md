
# Reading data from Parquet files into Dataframe
This document describes the methods for reading Parquet data into AWS Glue, utilizing both direct S3 access and the AWS Glue Data Catalog. The aim is to provide a comprehensive overview of handling both standard Parquet and Snappy-compressed Parquet formats to ensure efficient data ingestion and processing.

* Method 1- Read from S3 - parquet data from S3 bucket to dynamic dataframe.
* Method 2: Utilize AWS Glue Data Catalog to manage metadata and read parquet data stored in S3.

## Objective:
- Format Handling: Manage both standard and Snappy-compressed Parquet file reads into dynamic dataframes.
- Data Transformation: Leverage AWS Glue's capabilities to transform and process Parquet data effectively.

## Prerequisites
- Input Sources:
  * Method 1 : S3 buckets s3://ti-author-data/customer-billing/parquet/, s3://ti-author-data/customer-billing/snappy-parquet/
  * Method 2: Data Catalog tables - customer_billing_parquet, customer_billing_snappy_parquet
- Output: Dynamic Dataframe results in cloudwatch
- Cralwers: Method 2 : "customer_billing_parquet", "customer_billing_parquet_snappy"

## PySpark Scripts:
### Method 1: [Read from S3](../glue-code/ti-pyspark-read-from-parquet.py)
### Method 2: [Read from Data Catalog](../glue-code/ti-pyspark-read-from-parquet-crawler.py)
## Method 1 
### Main Operations
### 1. Initializing Spark and Glue Contexts:
* objective:Set up the necessary Spark and AWS Glue contexts to facilitate data manipulation.
* Implemnetation:
  ```python
  from pyspark.context import SparkContext
  from awsglue.context import GlueContext
  sc = SparkContext()
  glueContext = GlueContext(sc)
  ```
### 2. Reading CSV Data from S3:

* Objective: Load standard and Snappy-compressed Parquet files directly from S3 into DynamicFrames.
* Implemnetation:
  ```python
  
  # Specify the S3 path to the Parquet and Snappy-compressed Parquet files
  parquet_path = "s3://ti-author-data/customer-billing/parquet/"
  snappy_parquet_path = "s3://ti-author-data/customer-billing/snappy-parquet/"

  # Read the regular Parquet file using DynamicFrame
  dynamic_frame_parquet = glueContext.create_dynamic_frame.from_options(
      connection_type="s3",
      connection_options={"paths": [parquet_path]},
      format="parquet"
  )
  
  # Read the Snappy-compressed Parquet file using DynamicFrame
  dynamic_frame_snappy_parquet = glueContext.create_dynamic_frame.from_options(
      connection_type="s3",
      connection_options={"paths": [snappy_parquet_path]},
      format="parquet"
  )
  ```
### 3. Data Transformation into DataFrames:

* Objective: Convert DynamicFrames into DataFrames to utilize advanced data processing capabilities of PySpark.
* Implemnetation:
  ```python
  df_parquet = dynamic_frame_parquet.toDF()
  df_snappy_parquet = dynamic_frame_snappy_parquet.toDF()

  ```
### 4. Displaying Dataframe Contents:

* Objective: Show the contents of the DataFrames to verify correct data loading and provide a preview of the data.
* Implemnetation:
  ```python
  print("Count of rows in regular Parquet:", df_parquet.show(5))
  print("Count of rows in Snappy Parquet:", df_snappy_parquet.show(5))

  ```
### 5. Job Initialization and Commitment:

* Objective: Properly initialize and commit the AWS Glue job to ensure all processes are executed as configured.
* Implemnetation:
  ```python
  job.init("manual-job-name", {})
  job.commit()
  ```
## Method 2
### Main Operations
### 1. Initializing Spark and Glue Contexts:
* objective:Set up the necessary Spark and AWS Glue contexts to facilitate data manipulation.
* Implemnetation:
  ```python
  from pyspark.context import SparkContext
  from awsglue.context import GlueContext
  sc = SparkContext()
  glueContext = GlueContext(sc)
  ```
### 2. Data Loading from Glue Data Catalog:

* Objective: Leverage the AWS Glue Data Catalog for loading data, simplifying data management and accessibility.
* Implemnetation:
  ```python
  dynamic_frame_pq = glueContext.create_dynamic_frame.from_catalog(
      database="glue_db", 
      table_name="customer_billing_parquet"
  )
  dynamic_frame_snappy_pq = glueContext.create_dynamic_frame.from_catalog(
      database="glue_db", 
      table_name="customer_billing_snappy_parquet"
  )


  ```
### 3. Conversion to DataFrames and Processing:

* Objective: Convert DynamicFrames into Spark DataFrames to utilize PySpark's full data processing capabilities and perform simple data operations like counts or previews.
* Implemnetation:
  ```python
   df_pq = dynamic_frame_pq.toDF()
   df_snappy_pq = dynamic_frame_snappy_pq.toDF()


  ```
### 4. Displaying Dataframe Contents:

* Objective: Objective: Display contents of the DataFrames to verify data integrity and provide immediate data insights.
* Implemnetation:
  ```python
  print("Reading Parquet Contents:", df_pq.show(5))
  print("Reading Snappy Parquet Contents:", df_snappy_pq

  ```
### 5. Job Initialization and Commitment:

* Objective: Properly initialize and commit the AWS Glue job to ensure all processes are executed as configured.
* Implemnetation:
  ```python
  job.init("manual-job-name", {})
  job.commit()
  ```


