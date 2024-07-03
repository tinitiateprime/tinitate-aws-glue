# Understanding Having with PySpark in AWS Glue:

This document provides a guide on using PySpark within AWS Glue to perform group by and count operations, and additionally, to apply filtering similar to the HAVING clause in SQL, specifically on an "electric_vehicles" dataset stored in Athena. The script sets up the necessary Spark and Glue contexts, loads data, groups it by vehicle make and model, counts occurrences, applies a filter on these counts, and displays the results in descending order.

## HAVING Clause:
The HAVING clause is used in SQL to filter the results of a query based on aggregate functions, such as COUNT, SUM, AVG, etc. It filters groups of rows after they have been aggregated, unlike the WHERE clause which filters rows before aggregation. This functionality is crucial for working with grouped data when conditions need to be applied to the results of aggregate functions.

## Prerequisites
Ensure proper configuration of IAM roles and S3 buckets and run necessary crawleras outlined here:
* [IAM Prerequisites](IAM-prerequisites.md)
* [S3 Data Generation](s3-data-generation.md)
* [Crawler Setup Instructions](set-up-instructions.md)
  
##  PySpark Script - [pyspark-having](../glue-code/ti-pyspark-having.py)
- Input tables          : electric_vehicle_population_data_csv
- Output files          : Cloudwatch logs
- Crawlers used         : purchase_crawler


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
  * Objective: Loads the "electric_vehicles" table from Athena into a DataFrame, preparing it for analysis.
  * Implementation:
    ```python
    grouped_df = glueContext.create_dynamic_frame.from_catalog(database="glue_db", table_name="electric_vehicles").toDF()
    ```
### 3. Data Aggregation and Filtering:
  * Objective: Group data by 'make' and 'model', count occurrences, and filter results based on a predefined threshold.
  * Implementation:
    ```python
    result_df = grouped_df.groupBy("make", "model").agg(count("*").alias("count"))
    result_df_filtered = result_df.filter(result_df["count"] > 1000)
    ```

### 4. Displaying Results:
  * Objective: Display the aggregated and filtered results directly in the console for immediate inspection and verification.
  * Implementation:
    ```python
    print("Filtered Results:")
    result_df_filtered.show()
    ```

### 5. Logging and Execution Verification
  * Objective: Log the completion of the data filtering operation and confirm the successful display of data.
  * Implementation:
    ```python
    glueContext.get_logger().info("Filtered data successfully displayed in the console.")
    ```
