# Handling Null and Non-Null Values
The script performs essential data cleansing operations by segregating null and non-null values within a specified column of the dataset. This process is critical for preparing the data for further analytics, ensuring that downstream processes such as reporting and data visualization are based on clean and accurate data.

## Prerequisites

Ensure proper configuration of IAM roles and S3 buckets and run necessary crawleras outlined here:
* [IAM Prerequisites](IAM-prerequisites.md)
* [S3 Data Generation](s3-data-generation.md)
* [Crawler Setup Instructions](set-up-instructions.md)
  


##  PySpark Script - [pyspark-null-checks](../glue-code/ti-pyspark-isnull-notnull.py)
- Input tables          : categories_csv
- Output                : Cloudwatch logs
- Crawlers used         : category_crawler


## Main Operations

### 1. Initializing Spark and Glue Contexts:
* Objective: Establishes the necessary Spark and Glue contexts for data manipulation with logging set to INFO to control verbosity.
* Code Example:
  ```python
  from pyspark.context import SparkContext
  from awsglue.context import GlueContext
  sc = SparkContext()
  sc.setLogLevel("INFO")
  glueContext = GlueContext(sc)
  ```

### 2. Data Embedding and Loading
* Objective: Embed and load sample data into a DataFrame with predefined schema including null values, suitable for demonstrating null handling.
* Implementation:
  ```python
   from pyspark.sql.types import StructType, StructField, IntegerType, StringType
  schema = StructType([
      StructField("category_id", IntegerType(), True),
      StructField("categoryname", StringType(), True),
      StructField("description", StringType(), True)
  ])
  data = [
      (1, "Electronics", "Devices and gadgets"),
      (2, None, None),
      (3, "Clothing", "Wearables"),
      (4, "Home and Garden", "Home essentials"),
      (5, None, "All items for the kitchen")
  ]
  df = spark.createDataFrame(data, schema=schema)

  ```
### 3. Null Value Handling:
* Objective: Filter the DataFrame to segregate records where the 'categoryname' column contains null and non-null values, and display these records.
* Implementation:
  ```python
  df_null = df.filter(col("categoryname").isNull())
  print("Displaying DataFrame with null values in 'categoryname':")
  df_null.show()
  
  df_not_null = df.filter(col("categoryname").isNotNull())
  print("Displaying DataFrame with non-null values in 'categoryname':")
  df_not_null.show()

  ```  
    
### 4. Logging and Execution Verification:
* Objective: Utilize GlueContext's logging capabilities to log the execution status and verify the correct handling of data.
* Implementation:
  ```python
  glueContext.get_logger().info("Displayed DataFrames with null and not null values for 'categoryname'.")
  ```


