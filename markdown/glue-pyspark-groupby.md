# Understanding GroupBy with PySpark in AWS Glue:

This document outlines the use of PySpark in AWS Glue for grouping and counting data stored in Athena, focusing on electric vehicle datasets. The script sets up the necessary Spark and Glue contexts, performs data aggregation based on certain attributes, and utilizes logging to track the process. 

## GroupBy Aggregation

GroupBy counts the number of occurrences for each group specified by one or more columns.

Below is a detailed breakdown of the script's components and operations.

## Prerequisites for the pyspark script execution

Ensure proper configuration of IAM roles and S3 buckets and run necessary crawleras outlined here:
* [IAM Prerequisites](IAM-prerequisites.md)
* [S3 Data Generation](s3-data-generation.md)
* [Crawler Setup Instructions](set-up-instructions.md)
  
## PySpark Script - [pyspark-groupby.py](../glue-code/ti-pyspark-groupby.py)
- Input tables         : electric_vehicle_population_data_csv in Data Catalog
- Output files         : cloudwatch logs
- Crawlers used        : electric_vechiles


## Main Operations
### 1. Context Initialization:
  - Objective: Establish necessary contexts for Spark and Glue operations and set appropriate log levels.
  - Implementation:
    ```python
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("INFO")
    glueContext = GlueContext(sc)
    ```
### 2. Data Loading and Preparation:
  - Objective: Load tables from Athena into Spark DataFrames, transforming them for aggregation
  - Implementation:
    ```python
    electric_vehicles_df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="electric_vehicle_population_data_csv").toDF()
    ```

### 3. Performing GroupBy and Displaying Results:
   - Objective: Execute GroupBy operations to aggregate data by 'make' and 'model', and display the results directly in the console.
   - Implementation:
      ```python
      result_df = electric_vehicles_df.groupBy("make", "model").agg(count("*").alias("count"))
      print("Aggregated Results:")
      result_df.show()
     ```
      
### 4. Logging and Output Verification:
   - Objective: Log operational details and confirm the success of displaying data in the console.
   - Implementatione:
       ```python
      logger = glueContext.get_logger()
      logger.info("Aggregated data displayed in console successfully.")
     ```
