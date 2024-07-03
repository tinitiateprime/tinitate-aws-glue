# Hierarchical Queries with PySpark in AWS Glue

This document details a PySpark script designed to handle hierarchical data structures within AWS Glue, ideal for scenarios like organizational charts or nested relationships. The script demonstrates two distinct methods for constructing and exploring hierarchical relationships: recursive joins and graph processing using GraphFrames.


## Hierarchical queries Overview:
Hierarchical queries are used to handle data where rows have a parent-child relationship within the same table. These queries are common in SQL, particularly with the use of constructs like "CONNECT BY" in Oracle. In PySpark, however, there isn’t a built-in equivalent function, so these relationships must be handled programmatically.

Implementing hierarchical queries in PySpark usually involves using recursive joins or using graph theory libraries like GraphFrames

### Method 1: Recursive Joins
This method involves manually coding the logic to perform recursive joins to resolve parent-child relationships. This approach can be resource-intensive and tricky to manage for very deep or very large hierarchies.
### Method 2: Using GraphFrames
GraphFrames is a Spark package that extends DataFrames to support graph processing. With GraphFrames, you can manage complex relationships and perform recursive queries much more naturally.

## Prerequisites
Ensure proper configuration of IAM roles and S3 buckets and run necessary crawleras outlined here:
* [IAM Prerequisites](IAM-prerequisites.md)
* [S3 Data Generation](s3-data-generation.md)
* [Crawler Setup Instructions](set-up-instructions.md)
  
##  PySpark Script - [pyspark-hierarchical](../glue-code/ti-pyspark-hierarchical.py)
* Input Table: Sample data in dataframe
* Output Formats: Displays the result in cloudwatch logs

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
### 2. Data Generation and Loading:
* Objective: Generate and load sample data into a DataFrame for processing.
* Implementation:
  ```python
    data = [
    ("1", "Alice", None),
    ("2", "Bob", "1"),
    ("3", "Charlie", "2"),
    ("4", "David", "2"),
    ("5", "Eve", "3")
  ]
  columns = ["employee_id", "name", "manager_id"]
  employees = spark.createDataFrame(data, schema=columns)
  employees.createOrReplaceTempView("employees")
  ```
### 3. Hierarchical Data Processing:
#### Method 1: Recursive Joins
* Objective: Utilize recursive self joins to simulate a hierarchical tree structure.
* Implemetation:
  ```python
  # Join with itself on manager_id = employee_id
  for i in range(1, 4):  # assuming a maximum hierarchy depth of 3
      df = df.join(employees.alias(f"lvl{i}"), 
                   col("df.manager_id") == col(f"lvl{i}.employee_id"), "left_outer") \
             .select(col("df.employee_id"), col("df.name"), col(f"lvl{i}.name").alias(f"manager_lvl_{i}"))
  print("Method 1 Output Dataframe:")
  ```
#### Method 2: GraphFrames
* Objective: Leverage GraphFrames to analyze hierarchical structures using graph algorithms that uses "BFS Approach" from vertices and edges.
* Implementation:
  ```python
  from graphframes import GraphFrame
  vertices = employees.withColumnRenamed("employee_id", "id")
  edges = employees.selectExpr("employee_id as src", "manager_id as dst").filter("dst is not null")
  g = GraphFrame(vertices, edges)
  results = g.bfs(fromExpr="id = '1'", toExpr="id = '5'", edgeFilter="src != dst")
  results.show(truncate=False)
   ```
### 4. Output Display and Validation:
* Objective: Display and validate the results of the hierarchical queries directly in the console.
* Implemnetation:
  ```python
  print("Displaying results for Method 1:")
  df.show(truncate=False)
  print("Displaying results for Method 2:")
  results.show(truncate=False)
  ```
