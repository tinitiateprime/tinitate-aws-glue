# Writing Data into PARQUET 
This guide demonstrates how to create sample data within AWS Glue, convert it to a DataFrame and a DynamicFrame, and then write it to S3 as both uncompressed and Snappy-compressed Parquet files. This process is essential for efficient data management and ensures that data is correctly formatted and stored for further analytical purposes.

Objective:

## Objective:
- Data Creation and Manipulation: Generate sample data and manipulate it using PySpark within AWS Glue.
- Data Output: Efficiently write data to S3 in both uncompressed and Snappy-compressed Parquet formats.

## Prerequisites:
- Input Sources: Sample data in dynamic dataframe/Spark dataframe.
- Output: S3 bucktes:
  * s3://ti-author-scripts/ti-author-glue-scripts/ti-glue-pyspark-scripts-outputs/ti-pyspark-write-to-csv-outputs/parquet/
  * s3://ti-author-scripts/ti-author-glue-scripts/ti-glue-pyspark-scripts-outputs/ti-pyspark-write-to-csv-outputs/parquet-snappy/

## PySpark Scripts - [Write to PARQUET](../glue-code/ti-pyspark-write-to-parquet.py)

## Main Operations
### 1. Initializing Spark and Glue Contexts:
* objective:Set up the necessary Spark and AWS Glue contexts to facilitate data manipulation.
* Implemnetation:
  ```python
  from awsglue.context import GlueContext
  from pyspark.context import SparkContext
  sc = SparkContext()
  glueContext = GlueContext(sc)
  ```
### 2. Sample Data Creation:

* Objective: Sample data is created using Row objects, which are then parallelized into a DataFrame to mimic real data.
* Implemnetation:
  ```python
  from pyspark.sql import Row
  data = [
      Row(name="John Doe", age=30, city="New York"),
      Row(name="Jane Smith", age=25, city="Los Angeles"),
      Row(name="Mike Johnson", age=35, city="Chicago")
  ]
  df = sc.parallelize(data).toDF()
  ```
### 3. Dataframe Coalescing:

* Objective: Coalescing the DataFrame to a single partition to streamline the output process, making the resulting file singular and easier to handle.
* Implemnetation:
  ```python
  df_single = df.coalesce(1)
  ```
### 4. Conversion to DynamicFrame:

* Objective: Convert the DataFrame to a DynamicFrame, which is the preferred format for data manipulation and output in AWS Glue.
* Implemnetation:
  ```python
  from awsglue.dynamicframe import DynamicFrame
  dynamic_df_single = DynamicFrame.fromDF(df_single, glueContext, "dynamic_df_single")
  ```
### 5. Writing Data to S3:

* Objective:  Store the data efficiently in S3, using both uncompressed and Snappy compression formats for Parquet files.
* Implemnetation:
  ```python
   # Write the data as uncompressed Parquet
  glueContext.write_dynamic_frame.from_options(
      frame=dynamic_df_single,
      connection_type="s3",
      connection_options={"path": output_dir_parquet},
      format="parquet",
      format_options={"compression": "uncompressed"}
  )
  
  # Write the data as Snappy-compressed Parquet
  glueContext.write_dynamic_frame.from_options(
      frame=dynamic_df_single,
      connection_type="s3",
      connection_options={"path": output_dir_parquet_snappy},
      format="parquet",
      format_options={"compression": "snappy"}
  )

  ```

### 6. Job Completion and Logging:

* Objective: Ensuring the job is successfully completed and logging the outcome. This is crucial for tracking and verifying that data is written as expected.
* Implemnetation:
  ```python
  print("Successfully written the data in S3")
  job.commit()
  ```





