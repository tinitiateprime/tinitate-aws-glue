# Managing Data Re Partitions with PySpark in AWS Glue
This documentation details a PySpark script used within AWS Glue to manage data partitions effectively. The script loads data, checks its current partitioning, adjusts the partition count, and writes the results to Amazon S3 in various formats. The main goal is to demonstrate handling large datasets with varying partitions to optimize read and write performance.

Managing Data Partitions with PySpark in AWS Glue
## Key Concepts:
1. Partitions: Logical division of data to facilitate parallel processing.
2. Repartitioning: Adjusting the number of partitions to optimize processing.
3. Parallel Writing: Writing data in parallel to reduce execution time and improve throughput.

## Prerequisites
Ensure proper configuration of IAM roles and S3 buckets and run necessary crawleras outlined here:
* [IAM Prerequisites](IAM-prerequisites.md)
* [S3 Data Generation](s3-data-generation.md)
* [Crawler Setup Instructions](set-up-instructions.md)
  
##  PySpark Script - [pyspark-repartitioning.py](../glue-code/ti-pyspark-repartitioning.py)
* Input Table: products_csv
* Output Formats: CSV, JSON, and Parquet files in S3 buckets.
* Crawlers Used : products_crawler

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
    df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="products_csv").toDF()
    ```
### 3. Manage Partitions:
  * Objective: Check the current number of partitions and adjust as needed for optimized data handling.
  * Implementation:
    ```python
    current_partitions = df.rdd.getNumPartitions()
    print("Current number of partitions:", current_partitions)
    new_partitions = current_partitions + 1
    print("Increased number of partitions:", new_partitions)
    ```

### 4. Parallel Writing to S3:
  * Objective: Utilize repartitioning to enhance parallel writing to S3, improving data write performance.
  * Implementation:
    ```python
    def write_data(df, path_suffix, repartition=None):
      if repartition is not None:
          df = df.repartition(repartition)
      
      df.write.format("parquet").mode("overwrite").save(base_path + path_suffix + "parquet/")
      df.write.format("json").mode("overwrite").save(base_path + path_suffix + "json/")
      df.write.format("csv").option("header", "true").mode("overwrite").save(base_path + path_suffix + "csv/")
    ```
  #### Explanation of Parallel Writing:
  - By default, each partition of the DataFrame results in a separate file when written to a storage system like S3. Adjusting the number of partitions can directly influence the number of output files, allowing for better control over parallelism and resource utilization. Before repartitioning output writes into one part as in below image.
    <img width="934" alt="repartition_1" src="https://github.com/sarutlaa/tinitiate-aws-glue/assets/141533429/e9978b2f-3e8c-4796-aa97-7b677674d68b">

  - For example, increasing partitions before writing can distribute the workload more evenly across multiple nodes, potentially decreasing the time taken to write large datasets. After repartitioning output writes into two part as in below image.
    <img width="931" alt="repartition_2" src="https://github.com/sarutlaa/tinitiate-aws-glue/assets/141533429/36c22e15-2c0d-4909-94cd-54e5df857aa1">

  
### 5. Monitoring and Logging:
  * Objective: Track the success and details of the data processing and writing operations.
  * Implementation:
    ```python
     print("Data successfully written with current and increased partitions to S3.")
    ```
