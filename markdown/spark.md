# Apache Spark Overview

* Open-Source and Distributed: Apache Spark is an open-source, distributed processing system utilized for big data workloads, which allows for data processing on a large scale across clustered computers.

* In-Memory Caching: It leverages in-memory caching and optimized execution strategies to provide high-speed analytic queries, making it efficient for handling very large datasets.

* Optimized Query Execution: Spark optimizes query execution, allowing it to perform fast data processing tasks against large datasets, regardless of their size.

* Versatile Development APIs: Provides APIs for multiple programming languages including Java, Scala, Python, and R, facilitating a wide range of big data applications and data analysis tasks.

* Supports Multiple Workloads: Capable of handling diverse workload requirements such as batch processing, interactive queries, real-time analytics, machine learning, and graph processing.

* Widely Adopted in Various Industries: Used by numerous organizations across different industries for critical data processing tasks. Notable adopters include FINRA, Yelp, Zillow, DataXu, Urban Institute, and CrowdStrike.

## SparK Architecture
![Spark-architecture](https://github.com/sarutlaa/tinitiate-aws-glue/assets/141533429/08cf1a59-46be-4ff8-b1e9-7d3e57084309)

### Spark Applications
#### Driver Process:
- Main Controller: Runs the main program and manages how tasks are split and assigned to executors.
- Task Scheduler: Breaks down the application into tasks and distributes these tasks to the executors.
- Resource Manager: In cluster mode, it requests and manages resources needed for the executors.
- Results Collector: Gathers the results from all the executors and delivers the final output of the application.
#### Executor Processes:

- Task Performer: Executes the tasks assigned by the driver and returns the results.
- Memory Manager: Stores data in memory for quick access, which helps in speeding up the processing.
- Error Handler: Manages and recovers from errors by re-executing failed tasks.
- Parallel Processing: Runs multiple tasks at the same time, enabling efficient processing across the cluster.

### Spark’s Language APIs
Apache Spark provides APIs for several programming languages, making it flexible for developers to use their preferred language for big data projects. Here's a simplified overview:

![Spark APIS](https://github.com/sarutlaa/tinitiate-aws-glue/assets/141533429/fffd1253-6972-40a3-8942-1d5f90dc276a)

- Scala: Being written in Scala, Spark’s native API is highly optimized and powerful, making it a natural choice for those familiar with Scala.

- Python: Through PySpark, Python users can easily access Spark’s capabilities, benefiting from Python's simplicity and its strong data science libraries.

- Java: Spark also offers a Java API, which is great for developers who prefer Java’s structure and strong typing.

- R: SparkR allows R users to leverage Spark for large-scale data analysis directly from the R console.

- SQL: With Spark SQL, users can run SQL queries to manipulate data and integrate seamlessly with other Spark operations.

### How Language APIs Work with Spark
All these language APIs interact with Spark through a common gateway known as SparkSession:

- SparkSession: This is the main entry point for running Spark applications. No matter which language you use, SparkSession lets you access Spark features.

- Language Transparency: When you program in Python or R, you don't need to worry about the underlying Java system. Your Python or R code is automatically converted into a form that Spark can execute on its Java-based system.

This setup makes it easy to work with big data in Spark without needing to learn Java or understand the details of the system’s backend.

### Spark Session
- Entry Point: Spark Session serves as the primary entry point for working with Apache Spark in applications written in Python, Scala, Java, and R.
- Unified Interface: It provides a unified interface for interacting with Spark's functionalities like data processing, SQL queries, machine learning, and streaming.
- Data Processing: You can use Spark Session to read data from various sources (e.g., CSV, JSON, databases), perform transformations and filtering, and write the results to different destinations.
- SQL Capabilities: Spark Session enables you to write SQL-like queries (Spark SQL) to analyze large datasets stored in distributed format.
- Machine Learning and Streaming: It integrates with Spark MLlib for machine learning tasks and Spark Streaming for real-time data processing.
- Configuration: You can configure Spark Session with various settings to control cluster connection, memory allocation, and execution behavior.
- Context Management: Spark Session manages the Spark context, which represents the connection to the Spark cluster and provides resources for computations.
- Multiple Sessions: While typically you'll use a single Spark Session per application, you can create new sessions with isolated configurations for specific purposes.
- Sample Spark Session : 
```python
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("DataFrame Example") \
    .getOrCreate()
```
### Spark DataFrames: A Distributed Spreadsheet for Big Data

Spark DataFrames are the workhorses of data manipulation in Apache Spark. Imagine a giant spreadsheet that can handle massive datasets spread across multiple computers. Each row represents a data record, and each column holds a specific feature or attribute. DataFrames offer a familiar table structure for you to:

- Static Schema: Spark DataFrames require a defined schema. The structure of the data must be known and consistent across the entire dataset, which facilitates high-performance operations.
- Performance: DataFrames are highly optimized for performance, utilizing Spark's Catalyst optimizer for query execution and Tungsten for physical execution.
- APIs: Offers rich APIs for complex data transformations, aggregations, and SQL operations, which are highly expressive and widely used in data processing and analytics.
- Interoperability: DataFrames support seamless conversion to and from RDDs and can be easily integrated with other Spark components like Spark SQL and MLlib.
- Sample dataframe :
```python
# Sample data: a list of tuples, each representing a row in the DataFrame
data = [
    ("James", "Smith", 30),
    ("Anna", "Rose", 41),
    ("Robert", "Williams", 62)
]

# Define the schema of the data - names of columns
columns = ["FirstName", "LastName", "Age"]

# Create a DataFrame from the data and columns
df = spark.createDataFrame(data, columns)

# Show the contents of the DataFrame
df.show()
```

### What is Lazy Evaluation?
Apache Spark utilizes a concept known as "lazy evaluation" which is central to its processing efficiency and optimization capabilities. Here’s a straightforward overview, focusing on the concepts of transformations and actions in Spark:

Lazy evaluation means that Spark delays the execution of operations until it is absolutely necessary. This approach allows Spark to optimize the entire data processing workflow by grouping operations and minimizing data movement across the cluster.

#### Transformations
Transformations are operations that create a new dataset from an existing one. Examples include map, filter, reduceByKey, and many more.
- Lazy Nature: When a transformation is called, it does not execute immediately. Instead, Spark creates a plan (called a Directed Acyclic Graph or DAG) of all the transformations that have been called up to that point.
- No Data Movement: Since transformations are lazy, no data is moved or processed until an action is performed. This helps in optimizing the data processing pipeline by reducing unnecessary operations.
#### Actions
- Actions are operations that trigger the execution of the data processing computations described by the transformations. Examples of actions include count, collect, save, and show.
- Trigger Execution: An action triggers the actual data processing tasks. Once an action is called, Spark looks at the DAG of transformations and decides the best way to execute these to get the result of the action.
- Data Movement: During the execution of an action, data is shuffled, moved, or computed across the cluster as necessary.

### Benefits of Lazy Evaluation
- Optimization: Spark’s ability to optimize the execution plan (like rearranging operations or combining tasks) is possible because of lazy evaluation. This results in more efficient processing.
- Reduced Overhead: By organizing data operations into stages and executing them only when necessary, Spark reduces the overhead and latency associated with executing numerous intermediate data operations.
- Adaptive Execution: Spark can adapt the execution plan based on the actual data and cluster conditions at runtime, which often leads to better utilization of resources and faster execution times.

### Spark Dataframe vs Glue Dynamic Dataframe

# Spark DataFrame vs AWS Glue Dynamic DataFrame

| Aspect          | Spark DataFrame                           | AWS Glue Dynamic DataFrame                |
|-----------------|-------------------------------------------|-------------------------------------------|
| **Definition**           | A distributed collection of data organized into named columns, similar to tables in a relational database. | An extension of Spark DataFrames specifically designed for AWS Glue, with additional dynamic capabilities. |
| **Schema Flexibility** | Fixed schema; must be defined upfront or inferred at read time. | Flexible schema; can adapt dynamically to changing data during processing. |
| **Error Handling** | Fails on data errors unless explicitly handled. | Continues processing even with some data errors, making it robust for messy data sources. |
| **Environment** | Works in any Spark-enabled environment.  | Specific to AWS Glue, integrates well with AWS services. |
| **Performance** | Generally faster due to static schema optimization. | May have some overhead due to dynamic schema handling. |
| **Use Cases**   | Ideal for structured data with a stable schema. | Better for ETL jobs where data schema might vary or evolve over time. |



