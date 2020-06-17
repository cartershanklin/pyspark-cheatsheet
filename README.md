PySpark Cheat Sheet
===================
This cheat sheet will help you learn PySpark and write PySpark apps faster. Everything in here is fully functional PySpark code you can run or adapt to your programs.

These snippets are licensed under the CC0 1.0 Universal License. That means you can freely copy and adapt these code snippets and you don't need to give attribution or include any notices.

The snippets below refer to DataFrames loaded from the "Auto MPG Data Set" available from the [UCI Machine Learning Repository](https://archive.ics.uci.edu/ml/datasets/auto+mpg). You can download that dataset or clone this repository to test the code yourself.

These snippets were tested against the Spark 2.4.4 API. This page was last updated 2020-06-17 07:21:14.

Make note of these helpful links:
- [Built-in Spark SQL Functions](https://spark.apache.org/docs/latest/api/sql/index.html)
- [PySpark SQL Functions Source](https://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/functions.html)

If you find this guide helpful and want an easy way to run Spark, check out [Oracle Cloud Infrastructure Data Flow](https://www.oracle.com/big-data/data-flow/), a fully-managed Spark service that lets you run Spark jobs at any scale with no administrative overhead. You can try Data Flow free.


Table of contents
=================

<!--ts-->
   * [Loading and Saving Data](#loading-and-saving-data)
      * [Load a DataFrame from CSV](#load-a-dataframe-from-csv)
      * [Load a DataFrame from a Tab Separated Value (TSV) file](#load-a-dataframe-from-a-tab-separated-value-tsv-file)
      * [Load a CSV file with a money column into a DataFrame](#load-a-csv-file-with-a-money-column-into-a-dataframe)
      * [Load a CSV file with complex dates into a DataFrame](#load-a-csv-file-with-complex-dates-into-a-dataframe)
      * [Provide the schema when loading a DataFrame from CSV](#provide-the-schema-when-loading-a-dataframe-from-csv)
      * [Configure security to read a CSV file from Oracle Cloud Infrastructure Object Storage](#configure-security-to-read-a-csv-file-from-oracle-cloud-infrastructure-object-storage)
      * [Save a DataFrame in Parquet format](#save-a-dataframe-in-parquet-format)
      * [Save a DataFrame in CSV format](#save-a-dataframe-in-csv-format)
      * [Save a DataFrame to CSV, overwriting existing data](#save-a-dataframe-to-csv-overwriting-existing-data)
      * [Save a DataFrame to CSV with a header](#save-a-dataframe-to-csv-with-a-header)
      * [Save a DataFrame in a single CSV file](#save-a-dataframe-in-a-single-csv-file)
      * [Save DataFrame as a dynamic partitioned table](#save-dataframe-as-a-dynamic-partitioned-table)
      * [Overwrite specific partitions](#overwrite-specific-partitions)
   * [DataFrame Operations](#dataframe-operations)
      * [Add a new column with to a DataFrame](#add-a-new-column-with-to-a-dataframe)
      * [Modify a DataFrame column](#modify-a-dataframe-column)
      * [Add a column with multiple conditions](#add-a-column-with-multiple-conditions)
      * [Add a constant column](#add-a-constant-column)
      * [Concatenate columns](#concatenate-columns)
      * [Drop a column](#drop-a-column)
      * [Change a column name](#change-a-column-name)
      * [Change multiple column names](#change-multiple-column-names)
      * [Select particular columns from a DataFrame](#select-particular-columns-from-a-dataframe)
      * [Create an empty dataframe with a specified schema](#create-an-empty-dataframe-with-a-specified-schema)
      * [Create a constant dataframe](#create-a-constant-dataframe)
      * [Convert String to Double](#convert-string-to-double)
      * [Convert String to Integer](#convert-string-to-integer)
      * [Get the size of a DataFrame](#get-the-size-of-a-dataframe)
      * [Get a DataFrame's number of partitions](#get-a-dataframe-s-number-of-partitions)
      * [Get data types of a DataFrame's columns](#get-data-types-of-a-dataframe-s-columns)
      * [Convert an RDD to Data Frame](#convert-an-rdd-to-data-frame)
      * [Print the contents of an RDD](#print-the-contents-of-an-rdd)
      * [Print the contents of a DataFrame](#print-the-contents-of-a-dataframe)
      * [Process each row of a DataFrame](#process-each-row-of-a-dataframe)
      * [DataFrame Map example](#dataframe-map-example)
      * [DataFrame Flatmap example](#dataframe-flatmap-example)
      * [Create a custom UDF](#create-a-custom-udf)
   * [Transforming Data](#transforming-data)
      * [Fill NULL values in specific columns](#fill-null-values-in-specific-columns)
      * [Fill NULL values with column average](#fill-null-values-with-column-average)
      * [Fill NULL values with group average](#fill-null-values-with-group-average)
      * [Convert string to date](#convert-string-to-date)
      * [Convert string to date with custom format](#convert-string-to-date-with-custom-format)
      * [Convert UNIX (seconds since epoch) timestamp to date](#convert-unix-seconds-since-epoch-timestamp-to-date)
      * [Unpack a DataFrame's JSON column to a new DataFrame](#unpack-a-dataframe-s-json-column-to-a-new-dataframe)
      * [Query a JSON column](#query-a-json-column)
   * [Sorting and Searching](#sorting-and-searching)
      * [Filter a column using a condition](#filter-a-column-using-a-condition)
      * [Filter based on a specific column value](#filter-based-on-a-specific-column-value)
      * [Filter based on an IN list](#filter-based-on-an-in-list)
      * [Filter based on a NOT IN list](#filter-based-on-a-not-in-list)
      * [Filter a Dataframe based on substring search](#filter-a-dataframe-based-on-substring-search)
      * [Filter based on a column's length](#filter-based-on-a-column-s-length)
      * [Multiple filter conditions](#multiple-filter-conditions)
      * [Sort DataFrame by a column](#sort-dataframe-by-a-column)
      * [Take the first N rows of a DataFrame](#take-the-first-n-rows-of-a-dataframe)
      * [Get distinct values of a column](#get-distinct-values-of-a-column)
      * [Remove duplicates](#remove-duplicates)
   * [Grouping](#grouping)
      * [Group and sort](#group-and-sort)
      * [Group by multiple columns](#group-by-multiple-columns)
      * [Aggregate multiple columns](#aggregate-multiple-columns)
      * [Aggregate multiple columns with custom orderings](#aggregate-multiple-columns-with-custom-orderings)
      * [Get the maximum of a column](#get-the-maximum-of-a-column)
      * [Sum a list of columns](#sum-a-list-of-columns)
      * [Sum a column](#sum-a-column)
      * [Aggregate all numeric columns](#aggregate-all-numeric-columns)
      * [Count unique after grouping](#count-unique-after-grouping)
      * [Count distinct values on all columns](#count-distinct-values-on-all-columns)
      * [Group by then filter on the count](#group-by-then-filter-on-the-count)
      * [Find the top N per row group (use N=1 for maximum)](#find-the-top-n-per-row-group-use-n-1-for-maximum)
      * [Group key/values into a list](#group-key-values-into-a-list)
      * [Compute a histogram](#compute-a-histogram)
   * [Joining DataFrames](#joining-dataframes)
      * [Join two DataFrames by column name](#join-two-dataframes-by-column-name)
      * [Join two DataFrames with an expression](#join-two-dataframes-with-an-expression)
      * [Multiple join conditions](#multiple-join-conditions)
      * [Various Spark join types](#various-spark-join-types)
      * [Concatenate two DataFrames](#concatenate-two-dataframes)
      * [Load multiple files into a single DataFrame](#load-multiple-files-into-a-single-dataframe)
      * [Subtract DataFrames](#subtract-dataframes)
   * [File Processing](#file-processing)
      * [Load Local File Details into a DataFrame](#load-local-file-details-into-a-dataframe)
      * [Load Files from Oracle Cloud Infrastructure into a DataFrame](#load-files-from-oracle-cloud-infrastructure-into-a-dataframe)
      * [Transform Many Images using Pillow](#transform-many-images-using-pillow)
   * [Handling Missing Data](#handling-missing-data)
      * [Filter rows with None or Null values](#filter-rows-with-none-or-null-values)
      * [Drop rows with Null values](#drop-rows-with-null-values)
      * [Count all Null or NaN values in a DataFrame](#count-all-null-or-nan-values-in-a-dataframe)
   * [Pandas](#pandas)
      * [Convert Spark DataFrame to Pandas DataFrame](#convert-spark-dataframe-to-pandas-dataframe)
      * [Convert N rows from a DataFrame to a Pandas DataFrame](#convert-n-rows-from-a-dataframe-to-a-pandas-dataframe)
      * [Define a UDAF with Pandas](#define-a-udaf-with-pandas)
      * [Define a Pandas Grouped Map Function](#define-a-pandas-grouped-map-function)
   * [Data Profiling](#data-profiling)
      * [Compute the number of NULLs across all columns](#compute-the-number-of-nulls-across-all-columns)
      * [Compute average values of all numeric columns](#compute-average-values-of-all-numeric-columns)
      * [Compute minimum values of all numeric columns](#compute-minimum-values-of-all-numeric-columns)
      * [Compute maximum values of all numeric columns](#compute-maximum-values-of-all-numeric-columns)
   * [Time Series](#time-series)
      * [Zero fill missing values in a timeseries](#zero-fill-missing-values-in-a-timeseries)
      * [First Time an ID is Seen](#first-time-an-id-is-seen)
      * [Running or Cumulative Sum](#running-or-cumulative-sum)
      * [Running or Cumulative Average](#running-or-cumulative-average)
   * [Performance](#performance)
      * [Coalesce DataFrame partitions](#coalesce-dataframe-partitions)
      * [Change DataFrame partitioning](#change-dataframe-partitioning)
      * [Increase Spark driver/executor heap space](#increase-spark-driver-executor-heap-space)
<!--te-->
    

Loading and Saving Data
=======================
Loading data into DataFrames from various formats, and saving it out again.

Load a DataFrame from CSV
-------------------------

```python
# See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html
# for a list of supported options.
df = spark.read.format("csv").option("header", True).load("data/auto-mpg.csv")
```


Load a DataFrame from a Tab Separated Value (TSV) file
------------------------------------------------------

```python
# See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html
# for a list of supported options.
df = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", "\t")
    .load("data/auto-mpg.tsv")
)
```


Load a CSV file with a money column into a DataFrame
----------------------------------------------------

```python
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DecimalType
from decimal import Decimal

# Load the text file.
df = (
    spark.read.format("csv")
    .option("header", True)
    .load("data/customer_spend.csv")
)

# Convert with a hardcoded custom UDF.
money_udf = udf(lambda x: Decimal(x[1:].replace(",", "")), DecimalType(8, 4))
money1 = df.withColumn("spend_dollars", money_udf(df.spend_dollars))

# Convert with the money_parser library (much safer).
from money_parser import price_str

money_convert = udf(
    lambda x: Decimal(price_str(x)) if x is not None else None,
    DecimalType(8, 4),
)
money2 = df.withColumn("spend_dollars", money_convert(df.spend_dollars))
```


Load a CSV file with complex dates into a DataFrame
---------------------------------------------------

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import TimestampType
import dateparser

# Use the dateparser module to convert many formats into timestamps.
date_convert = udf(
    lambda x: dateparser.parse(x) if x is not None else None, TimestampType()
)
df = (
    spark.read.format("csv")
    .option("header", True)
    .load("data/date_examples.csv")
)
df = df.withColumn("parsed", date_convert(df.date))
```


Provide the schema when loading a DataFrame from CSV
----------------------------------------------------

```python
# See https://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/types.html
# for a list of types.
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

schema = StructType(
    [
        StructField("mpg", DoubleType(), True),
        StructField("cylinders", IntegerType(), True),
        StructField("displacement", DoubleType(), True),
        StructField("horsepower", DoubleType(), True),
        StructField("weight", DoubleType(), True),
        StructField("acceleration", DoubleType(), True),
        StructField("modelyear", IntegerType(), True),
        StructField("origin", IntegerType(), True),
        StructField("carname", StringType(), True),
    ]
)
df = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(schema)
    .load("data/auto-mpg.csv")
)
```


Configure security to read a CSV file from Oracle Cloud Infrastructure Object Storage
-------------------------------------------------------------------------------------

```python
import oci

oci_config = oci.config.from_file()
conf.set("fs.oci.client.auth.tenantId", oci_config["tenancy"])
conf.set("fs.oci.client.auth.userId", oci_config["user"])
conf.set("fs.oci.client.auth.fingerprint", oci_config["fingerprint"])
conf.set("fs.oci.client.auth.pemfilepath", oci_config["key_file"])
conf.set(
    "fs.oci.client.hostname",
    "https://objectstorage.{0}.oraclecloud.com".format(oci_config["region"]),
)
PATH = "oci://<your_bucket>@<your_namespace/<your_path>"
df = spark.read.format("csv").option("header", True).load(PATH)
```


Save a DataFrame in Parquet format
----------------------------------

```python
df.write.parquet("output.parquet")
```


Save a DataFrame in CSV format
------------------------------

```python
# See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameWriter.html
# for a list of supported options.
df.write.csv("output.csv")
```


Save a DataFrame to CSV, overwriting existing data
--------------------------------------------------

```python
df.write.mode("overwrite").csv("output.csv")
```


Save a DataFrame to CSV with a header
-------------------------------------

```python
# See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameWriter.html
# for a list of supported options.
df.coalesce(1).write.csv("header.csv", header="true")
```


Save a DataFrame in a single CSV file
-------------------------------------

```python
df.coalesce(1).write.csv("single.csv")
```


Save DataFrame as a dynamic partitioned table
---------------------------------------------

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("append").partitionBy("modelyear").saveAsTable("autompg")
```


Overwrite specific partitions
-----------------------------

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
data.write.mode("overwrite").insertInto("my_table")
```


DataFrame Operations
====================
Adding, removing and modifying DataFrame columns.

Add a new column with to a DataFrame
------------------------------------

```python
from pyspark.sql.functions import upper, lower

df = df.withColumn("upper", upper(df.carname)).withColumn(
    "lower", lower(df.carname)
)
```


Modify a DataFrame column
-------------------------

```python
from pyspark.sql.functions import col, concat, lit

df = df.withColumn("modelyear", concat(lit("19"), col("modelyear")))
```


Add a column with multiple conditions
-------------------------------------

```python
from pyspark.sql.functions import col, when

df = df.withColumn(
    "mpg_class",
    when(col("mpg") <= 20, "low")
    .when(col("mpg") <= 30, "mid")
    .when(col("mpg") <= 40, "high")
    .otherwise("very high"),
)
```


Add a constant column
---------------------

```python
from pyspark.sql.functions import lit

df = df.withColumn("one", lit(1))
```


Concatenate columns
-------------------

```python
from pyspark.sql.functions import concat, col, lit

df = df.withColumn(
    "concatenated", concat(col("cylinders"), lit("_"), col("mpg"))
)
```


Drop a column
-------------

```python
df = df.drop("horsepower")
```


Change a column name
--------------------

```python
df = df.withColumnRenamed("horsepower", "horses")
```


Change multiple column names
----------------------------

```python
df = df.withColumnRenamed("horsepower", "horses").withColumnRenamed(
    "modelyear", "year"
)
```


Select particular columns from a DataFrame
------------------------------------------

```python
df = df.select(["mpg", "cylinders", "displacement"])
```


Create an empty dataframe with a specified schema
-------------------------------------------------

```python
from pyspark.sql.types import StructField, StructType, LongType, StringType

schema = StructType(
    [
        StructField("my_id", LongType(), True),
        StructField("my_string", StringType(), True),
    ]
)
df = spark.createDataFrame([], schema)
```


Create a constant dataframe
---------------------------

```python
import datetime
from pyspark.sql.types import (
    StructField,
    StructType,
    LongType,
    StringType,
    TimestampType,
)

schema = StructType(
    [
        StructField("my_id", LongType(), True),
        StructField("my_string", StringType(), True),
        StructField("my_timestamp", TimestampType(), True),
    ]
)
df = spark.createDataFrame(
    [
        (1, "foo", datetime.datetime.strptime("2021-01-01", "%Y-%m-%d")),
        (2, "bar", datetime.datetime.strptime("2021-01-02", "%Y-%m-%d")),
    ],
    schema,
)
```


Convert String to Double
------------------------

```python
from pyspark.sql.functions import col

df = df.withColumn("horsepower", col("horsepower").cast("double"))
```


Convert String to Integer
-------------------------

```python
from pyspark.sql.functions import col

df = df.withColumn("horsepower", col("horsepower").cast("int"))
```


Get the size of a DataFrame
---------------------------

```python
print("{} rows".format(df.count()))
print("{} columns".format(len(df.columns)))
```


Get a DataFrame's number of partitions
--------------------------------------

```python
print("{} partition(s)".format(df.rdd.getNumPartitions()))
```


Get data types of a DataFrame's columns
---------------------------------------

```python
print(df.dtypes)
```


Convert an RDD to Data Frame
----------------------------

```python
from pyspark.sql import Row

row = Row("val")
df = rdd.map(row).toDF()
```


Print the contents of an RDD
----------------------------

```python
print(rdd.take(10))
```


Print the contents of a DataFrame
---------------------------------

```python
df.show(10)
```


Process each row of a DataFrame
-------------------------------

```python
import os

def foreach_function(row):
    if row.horsepower is not None:
        os.system("echo " + row.horsepower)

df.foreach(foreach_function)
```


DataFrame Map example
---------------------

```python
def map_function(row):
    if row.horsepower is not None:
        return [float(row.horsepower) * 10]
    else:
        return [None]

df = df.rdd.map(map_function).toDF()
```


DataFrame Flatmap example
-------------------------

```python
from pyspark.sql.types import Row

def flatmap_function(row):
    if row.cylinders is not None:
        return list(range(int(row.cylinders)))
    else:
        return [None]

rdd = df.rdd.flatMap(flatmap_function)
row = Row("val")
df = rdd.map(row).toDF()
```


Create a custom UDF
-------------------

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

first_word_udf = udf(lambda x: x.split()[0], StringType())
df = df.withColumn("manufacturer", first_word_udf(df.carname))
```


Transforming Data
=================
Data conversions and other modifications.

Fill NULL values in specific columns
------------------------------------

```python
df.fillna({"horsepower": 0})
```


Fill NULL values with column average
------------------------------------

```python
from pyspark.sql.functions import avg

df.fillna({"horsepower": df.agg(avg("horsepower")).first()[0]})
```


Fill NULL values with group average
-----------------------------------

```python
from pyspark.sql.functions import avg, coalesce

unmodified_columns = df.columns
unmodified_columns.remove("horsepower")
manufacturer_avg = df.groupBy("cylinders").agg({"horsepower": "avg"})
df = df.join(manufacturer_avg, "cylinders").select(
    *unmodified_columns,
    coalesce("horsepower", "avg(horsepower)").alias("horsepower")
)
```


Convert string to date
----------------------

```python
from pyspark.sql.functions import col

df = spark.sparkContext.parallelize([["2021-01-01"], ["2022-01-01"]]).toDF(
    ["date_col"]
)
df = df.withColumn("date_col", col("date_col").cast("date"))
```


Convert string to date with custom format
-----------------------------------------

```python
from pyspark.sql.functions import col, to_date

df = spark.sparkContext.parallelize([["20210101"], ["20220101"]]).toDF(
    ["date_col"]
)
df = df.withColumn("date_col", to_date(col("date_col"), "yyyyddMM"))
```


Convert UNIX (seconds since epoch) timestamp to date
----------------------------------------------------

```python
from pyspark.sql.functions import col, from_unixtime

df = spark.sparkContext.parallelize([["1590183026"], ["2000000000"]]).toDF(
    ["ts_col"]
)
df = df.withColumn("date_col", from_unixtime(col("ts_col")))
```


Unpack a DataFrame's JSON column to a new DataFrame
---------------------------------------------------

```python
from pyspark.sql.functions import col, explode, from_json, json_tuple

source = spark.sparkContext.parallelize(
    [["1", '{ "a" : 10, "b" : 11 }'], ["2", '{ "a" : 20, "b" : 21 }']]
).toDF(["id", "json"])
df = source.select("id", json_tuple(col("json"), "a", "b"))
```


Query a JSON column
-------------------

```python
from pyspark.sql.functions import col, explode, from_json, json_tuple

source = spark.sparkContext.parallelize(
    [["1", '{ "a" : 10, "b" : 11 }'], ["2", '{ "a" : 20, "b" : 21 }']]
).toDF(["id", "json"])
filtered = (
    source.select("id", json_tuple(col("json"), "a", "b"))
    .withColumnRenamed("c0", "a")
    .withColumnRenamed("c1", "b")
    .where(col("b") > 15)
)
```


Sorting and Searching
=====================
Filtering, sorting, removing duplicates and more.

Filter a column using a condition
---------------------------------

```python
from pyspark.sql.functions import col

filtered = df.filter(col("mpg") > "30")
```


Filter based on a specific column value
---------------------------------------

```python
from pyspark.sql.functions import col

filtered = df.where(col("cylinders") == "8")
```


Filter based on an IN list
--------------------------

```python
from pyspark.sql.functions import col

filtered = df.where(col("cylinders").isin(["4", "6"]))
```


Filter based on a NOT IN list
-----------------------------

```python
from pyspark.sql.functions import col

filtered = df.where(~col("cylinders").isin(["4", "6"]))
```


Filter a Dataframe based on substring search
--------------------------------------------

```python
from pyspark.sql.functions import col

filtered = df.where(col("carname").like("%custom%"))
```


Filter based on a column's length
---------------------------------

```python
from pyspark.sql.functions import col, length

filtered = df.where(length(col("carname")) < 12)
```


Multiple filter conditions
--------------------------

```python
from pyspark.sql.functions import col

or_conditions = df.filter((col("mpg") > "30") | (col("acceleration") < "10"))
and_conditions = df.filter((col("mpg") > "30") & (col("acceleration") < "13"))
```


Sort DataFrame by a column
--------------------------

```python
from pyspark.sql.functions import col

ascending = df.orderBy("carname")
descending = df.orderBy(col("carname").desc())
```


Take the first N rows of a DataFrame
------------------------------------

```python
n = 10
reduced = df.limit(n)
```


Get distinct values of a column
-------------------------------

```python
distinct = df.select("cylinders").distinct()
```


Remove duplicates
-----------------

```python
filtered = df.dropDuplicates(["carname"])
```


Grouping
========
Group DataFrame data by key to perform aggregats like sums, averages, etc.

Group and sort
--------------

```python
from pyspark.sql.functions import avg, desc

grouped = (
    df.groupBy("cylinders")
    .agg(avg("horsepower").alias("avg_horsepower"))
    .orderBy(desc("avg_horsepower"))
)
```


Group by multiple columns
-------------------------

```python
from pyspark.sql.functions import avg, desc

grouped = (
    df.groupBy(["modelyear", "cylinders"])
    .agg(avg("horsepower").alias("avg_horsepower"))
    .orderBy(desc("avg_horsepower"))
)
```


Aggregate multiple columns
--------------------------

```python
from pyspark.sql.functions import asc, desc_nulls_last

expressions = dict(horsepower="avg", weight="max", displacement="max")
grouped = df.groupBy("modelyear").agg(expressions)
```


Aggregate multiple columns with custom orderings
------------------------------------------------

```python
from pyspark.sql.functions import asc, desc_nulls_last

expressions = dict(horsepower="avg", weight="max", displacement="max")
orderings = [
    desc_nulls_last("max(displacement)"),
    desc_nulls_last("avg(horsepower)"),
    asc("max(weight)"),
]
grouped = df.groupBy("modelyear").agg(expressions).orderBy(*orderings)
```


Get the maximum of a column
---------------------------

```python
from pyspark.sql.functions import col, max

grouped = df.select(max(col("horsepower")).alias("max_horsepower"))
```


Sum a list of columns
---------------------

```python
exprs = {x: "sum" for x in ("weight", "cylinders", "mpg")}
summed = df.agg(exprs)
```


Sum a column
------------

```python
from pyspark.sql.functions import sum

grouped = df.groupBy("cylinders").agg(sum("weight").alias("total_weight"))
```


Aggregate all numeric columns
-----------------------------

```python
numerics = set(["decimal", "double", "float", "integer", "long", "short"])
exprs = {x[0]: "sum" for x in df.dtypes if x[1] in numerics}
summed = df.agg(exprs)
```


Count unique after grouping
---------------------------

```python
from pyspark.sql.functions import countDistinct

grouped = df.groupBy("cylinders").agg(countDistinct("mpg"))
```


Count distinct values on all columns
------------------------------------

```python
from pyspark.sql.functions import countDistinct

grouped = df.agg(*(countDistinct(c) for c in df.columns))
```


Group by then filter on the count
---------------------------------

```python
from pyspark.sql.functions import col

grouped = df.groupBy("cylinders").count().where(col("count") > 100)
```


Find the top N per row group (use N=1 for maximum)
--------------------------------------------------

```python
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# To get the maximum per group, set n=1.
n = 5
w = Window().partitionBy("cylinders").orderBy(col("horsepower").desc())
result = (
    df.withColumn("horsepower", col("horsepower").cast("double"))
    .withColumn("rn", row_number().over(w))
    .where(col("rn") <= n)
    .select("*")
)
```


Group key/values into a list
----------------------------

```python
from pyspark.sql.functions import col, collect_list

collected = df.groupBy("cylinders").agg(
    collect_list(col("carname")).alias("models")
)
```


Compute a histogram
-------------------

```python
from pyspark.sql.functions import col

# Target column must be numeric.
df = df.withColumn("horsepower", col("horsepower").cast("double"))

# N is the number of bins.
N = 11
histogram = df.select("horsepower").rdd.flatMap(lambda x: x).histogram(N)
```


Joining DataFrames
==================
Joining and stacking DataFrames.

Join two DataFrames by column name
----------------------------------

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Load a list of manufacturer / country pairs.
countries = (
    spark.read.format("csv")
    .option("header", True)
    .load("data/manufacturers.csv")
)

# Add a manufacturers column, to join with the manufacturers list.
first_word_udf = udf(lambda x: x.split()[0], StringType())
df = df.withColumn("manufacturer", first_word_udf(df.carname))

# The actual join.
joined = df.join(countries, "manufacturer")
```


Join two DataFrames with an expression
--------------------------------------

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Load a list of manufacturer / country pairs.
countries = (
    spark.read.format("csv")
    .option("header", True)
    .load("data/manufacturers.csv")
)

# Add a manufacturers column, to join with the manufacturers list.
first_word_udf = udf(lambda x: x.split()[0], StringType())
df = df.withColumn("manufacturer", first_word_udf(df.carname))

# The actual join.
joined = df.join(countries, df.manufacturer == countries.manufacturer)
```


Multiple join conditions
------------------------

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Load a list of manufacturer / country pairs.
countries = (
    spark.read.format("csv")
    .option("header", True)
    .load("data/manufacturers.csv")
)

# Add a manufacturers column, to join with the manufacturers list.
first_word_udf = udf(lambda x: x.split()[0], StringType())
df = df.withColumn("manufacturer", first_word_udf(df.carname))

# The actual join.
joined = df.join(
    countries,
    (df.manufacturer == countries.manufacturer)
    | (df.mpg == countries.manufacturer),
)
```


Various Spark join types
------------------------

```python
# Inner join on one column.
joined = df.join(df, "carname")

# Left (outer) join.
joined = df.join(df, "carname", "left")

# Left anti (not in) join.
joined = df.join(df, "carname", "left_anti")

# Right (outer) join.
joined = df.join(df, "carname", "right")

# Full join.
joined = df.join(df, "carname", "full")

# Cross join.
joined = df.crossJoin(df)
```


Concatenate two DataFrames
--------------------------

```python
df1 = spark.read.format("csv").option("header", True).load("data/part1.csv")
df2 = spark.read.format("csv").option("header", True).load("data/part2.csv")
df = df1.union(df2)
```


Load multiple files into a single DataFrame
-------------------------------------------

```python
# Approach 1: Use a list.
df = (
    spark.read.format("csv")
    .option("header", True)
    .load(["data/part1.csv", "data/part2.csv"])
)

# Approach 2: Use a wildcard.
df = spark.read.format("csv").option("header", True).load("data/part*.csv")
```


Subtract DataFrames
-------------------

```python
from pyspark.sql.functions import col

reduced = df.subtract(df.where(col("mpg") < "25"))
```


File Processing
===============
Loading File Metadata and Processing Files

Load Local File Details into a DataFrame
----------------------------------------

```python
from pyspark.sql.types import (
    StructField,
    StructType,
    LongType,
    StringType,
    TimestampType,
)
import datetime
import os

# Simple: Use glob and only file names.
files = [[x] for x in glob.glob("/etc/*")]
df = spark.createDataFrame(files)

# Advanced: Use os.walk and extended attributes.
target_path = "/etc"
entries = []
walker = os.walk(target_path)
for root, dirs, files in walker:
    for file in files:
        full_path = os.path.join(root, file)
        try:
            stat_info = os.stat(full_path)
            entries.append(
                [
                    file,
                    full_path,
                    stat_info.st_size,
                    datetime.datetime.fromtimestamp(stat_info.st_mtime),
                ]
            )
        except:
            pass
schema = StructType(
    [
        StructField("file", StringType(), False),
        StructField("path", StringType(), False),
        StructField("size", LongType(), False),
        StructField("mtime", TimestampType(), False),
    ]
)
df = spark.createDataFrame(entries, schema)
```


Load Files from Oracle Cloud Infrastructure into a DataFrame
------------------------------------------------------------

```python
from pyspark.sql.types import (
    StructField,
    StructType,
    LongType,
    StringType,
    TimestampType,
)
import datetime

# Requires an object_store_client object.
# See https://oracle-cloud-infrastructure-python-sdk.readthedocs.io/en/latest/api/object_storage/client/oci.object_storage.ObjectStorageClient.html
input_bucket = "input"
raw_inputs = object_store_client.list_objects(
    object_store_client.get_namespace().data, input_bucket
)
files = [
    [
        x.name,
        x.size,
        datetime.datetime.fromtimestamp(x.time_modified)
        if x.time_modified is not None
        else None,
    ]
    for x in raw_inputs.data.objects
]
schema = StructType(
    [
        StructField("name", StringType(), False),
        StructField("size", LongType(), True),
        StructField("modified", TimestampType(), True),
    ]
)
df = spark.createDataFrame(files, schema)
```


Transform Many Images using Pillow
----------------------------------

```python
from PIL import Image
from pyspark.sql.types import StructField, StructType, StringType
import glob

def resize_an_image(row):
    width, height = 128, 128
    file_name = row._1
    new_name = file_name.replace(".png", ".resized.png")
    img = Image.open(file_name)
    img = img.resize((width, height), Image.ANTIALIAS)
    img.save(new_name)

files = [[x] for x in glob.glob("data/*.png")]
df = spark.createDataFrame(files)
df.foreach(resize_an_image)
```


Handling Missing Data
=====================
Dealing with NULLs and NaNs in DataFrames.

Filter rows with None or Null values
------------------------------------

```python
from pyspark.sql.functions import col

filtered = df.where(col("horsepower").isNull())
filtered = df.where(col("horsepower").isNotNull())
```


Drop rows with Null values
--------------------------

```python
# thresh controls the number of nulls before the row gets dropped.
# subset controls the columns to consider.
df = df.na.drop(thresh=2, subset=("horsepower",))
```


Count all Null or NaN values in a DataFrame
-------------------------------------------

```python
from pyspark.sql.functions import col, count, isnan, when

result = df.select([count(when(isnan(c), c)).alias(c) for c in df.columns])
result = df.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
)
```


Pandas
======
Using Python's Pandas library to augment Spark. Some operations require the pyarrow library.

Convert Spark DataFrame to Pandas DataFrame
-------------------------------------------

```python
pdf = df.toPandas()
```


Convert N rows from a DataFrame to a Pandas DataFrame
-----------------------------------------------------

```python
N = 10
pdf = df.limit(N).toPandas()
```


Define a UDAF with Pandas
-------------------------

```python
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import col

@pandas_udf("double", PandasUDFType.GROUPED_AGG)
def mean_udaf(pdf):
    return pdf.mean()

df = df.withColumn("mpg", col("mpg").cast("double"))
df = df.groupby("cylinders").agg(mean_udaf(df["mpg"]))
```


Define a Pandas Grouped Map Function
------------------------------------

```python
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import col

df = df.withColumn("horsepower", col("horsepower").cast("double"))

@pandas_udf(df.schema, PandasUDFType.GROUPED_MAP)
def rescale(pdf):
    minv = pdf.horsepower.min()
    maxv = pdf.horsepower.max() - minv
    return pdf.assign(horsepower=(pdf.horsepower - minv) / maxv * 100)

df = df.groupby("cylinders").apply(rescale)
```


Data Profiling
==============
Extracting key statistics out of a body of data.

Compute the number of NULLs across all columns
----------------------------------------------

```python
from pyspark.sql.functions import col, count, when

result = df.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
)
```


Compute average values of all numeric columns
---------------------------------------------

```python
numerics = set(["decimal", "double", "float", "integer", "long", "short"])
exprs = {x[0]: "avg" for x in df.dtypes if x[1] in numerics}
result = df.agg(exprs)
```


Compute minimum values of all numeric columns
---------------------------------------------

```python
numerics = set(["decimal", "double", "float", "integer", "long", "short"])
exprs = {x[0]: "min" for x in df.dtypes if x[1] in numerics}
result = df.agg(exprs)
```


Compute maximum values of all numeric columns
---------------------------------------------

```python
numerics = set(["decimal", "double", "float", "integer", "long", "short"])
exprs = {x[0]: "max" for x in df.dtypes if x[1] in numerics}
result = df.agg(exprs)
```


Time Series
===========
Techniques for dealing with time series data.

Zero fill missing values in a timeseries
----------------------------------------

```python
from pyspark.sql.functions import coalesce, lit

# Use distinct values of customer and date from the dataset itself.
# In general it's safer to use known reference tables for IDs and dates.
filled = df.join(
    df.select("customer_id").distinct().crossJoin(df.select("date").distinct()),
    ["date", "customer_id"],
    "right",
).select("date", "customer_id", coalesce("spend_dollars", lit(0)))
```


First Time an ID is Seen
------------------------

```python
from pyspark.sql.functions import first
from pyspark.sql.window import Window

w = Window().partitionBy("customer_id").orderBy("date")
df = df.withColumn("first_seen", first("date").over(w))
```


Running or Cumulative Sum
-------------------------

```python
from pyspark.sql.functions import col, sum
from pyspark.sql.window import Window

w = (
    Window()
    .partitionBy("customer_id")
    .orderBy("date")
    .rangeBetween(Window.unboundedPreceding, 0)
)
df = df.withColumn(
    "spend_dollars", col("spend_dollars").cast("double")
).withColumn("running_sum", sum("spend_dollars").over(w))
```


Running or Cumulative Average
-----------------------------

```python
from pyspark.sql.functions import avg, col
from pyspark.sql.window import Window

w = (
    Window()
    .partitionBy("customer_id")
    .orderBy("date")
    .rangeBetween(Window.unboundedPreceding, 0)
)
df = df.withColumn(
    "spend_dollars", col("spend_dollars").cast("double")
).withColumn("running_avg", avg("spend_dollars").over(w))
```


Performance
===========
A few performance tips and tricks.

Coalesce DataFrame partitions
-----------------------------

```python
import math

target_partitions = math.ceil(df.rdd.getNumPartitions() / 2)
df = df.coalesce(target_partitions)
```


Change DataFrame partitioning
-----------------------------

```python
from pyspark.sql.functions import col

df = df.repartition(col("modelyear"))
number_of_partitions = 5
df = df.repartitionByRange(number_of_partitions, col("mpg"))
```


Increase Spark driver/executor heap space
-----------------------------------------

```python
# Memory configuration depends entirely on your runtime.
# In OCI Data Flow you control memory by selecting a larger or smaller VM.
# No other configuration is needed.
#
# For other environments see the Spark "Cluster Mode Overview" to get started.
# https://spark.apache.org/docs/latest/cluster-overview.html
# And good luck!
```
