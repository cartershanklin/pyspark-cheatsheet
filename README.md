PySpark Cheat Sheet
===================
This cheat sheet will help you learn PySpark and write PySpark apps faster. Everything in here is fully functional PySpark code you can run or adapt to your programs.

These snippets below refer to DataFrames loaded from the "Auto MPG Data Set" available from the [UCI Machine Learning Repository](https://archive.ics.uci.edu/ml/datasets/auto+mpg). You can download that dataset or clone this repository to test the code yourself.

These snippets were developed against the Spark 2.4.4 API. This page was last updated 2020-05-30 08:35:50.


Table of contents
=================

<!--ts-->
   * [Loading and Saving Data](#loading-and-saving-data)
      * [Save a DataFrame in Parquet format](#save-a-dataframe-in-parquet-format)
      * [Load a DataFrame from CSV](#load-a-dataframe-from-csv)
      * [Save a DataFrame in CSV format](#save-a-dataframe-in-csv-format)
      * [Save a DataFrame in a single CSV file](#save-a-dataframe-in-a-single-csv-file)
      * [Save a DataFrame to CSV with a header](#save-a-dataframe-to-csv-with-a-header)
      * [Save a DataFrame to CSV, overwriting existing data](#save-a-dataframe-to-csv-overwriting-existing-data)
      * [Provide the schema when loading a DataFrame from CSV](#provide-the-schema-when-loading-a-dataframe-from-csv)
      * [Save DataFrame as a dynamic partitioned table](#save-dataframe-as-a-dynamic-partitioned-table)
      * [Read a CSV file from OCI Object Storage](#read-a-csv-file-from-oci-object-storage)
      * [Overwrite specific partitions](#overwrite-specific-partitions)
   * [DataFrame Operations](#dataframe-operations)
      * [Modify a DataFrame column](#modify-a-dataframe-column)
      * [Add a new column with builtin UDF](#add-a-new-column-with-builtin-udf)
      * [Create a custom UDF](#create-a-custom-udf)
      * [Concatenate columns](#concatenate-columns)
      * [Convert String to Double](#convert-string-to-double)
      * [Convert String to Integer](#convert-string-to-integer)
      * [Change a column name](#change-a-column-name)
      * [Change multiple column names](#change-multiple-column-names)
      * [Convert an RDD to Data Frame](#convert-an-rdd-to-data-frame)
      * [Create an empty dataframe with a specified schema](#create-an-empty-dataframe-with-a-specified-schema)
      * [Drop a column](#drop-a-column)
      * [Print the contents of an RDD](#print-the-contents-of-an-rdd)
      * [Print the contents of a DataFrame](#print-the-contents-of-a-dataframe)
      * [Add a column with multiple conditions](#add-a-column-with-multiple-conditions)
      * [Add a constant column](#add-a-constant-column)
      * [Process each row of a DataFrame](#process-each-row-of-a-dataframe)
      * [DataFrame Map example](#dataframe-map-example)
      * [DataFrame Flatmap example](#dataframe-flatmap-example)
      * [Create a constant dataframe](#create-a-constant-dataframe)
      * [Select particular columns from a DataFrame](#select-particular-columns-from-a-dataframe)
      * [Get the size of a DataFrame](#get-the-size-of-a-dataframe)
      * [Get a DataFrame's number of partitions](#get-a-dataframe-s-number-of-partitions)
      * [Get data types of a DataFrame's columns](#get-data-types-of-a-dataframe-s-columns)
   * [Transforming Data](#transforming-data)
      * [Convert string to date](#convert-string-to-date)
      * [Convert string to date with custom format](#convert-string-to-date-with-custom-format)
      * [Convert UNIX (seconds since epoch) timestamp to date](#convert-unix-seconds-since-epoch-timestamp-to-date)
      * [Fill NULL values in specific columns](#fill-null-values-in-specific-columns)
      * [Fill NULL values with column average](#fill-null-values-with-column-average)
      * [Fill NULL values with group average](#fill-null-values-with-group-average)
      * [Unpack a DataFrame's JSON column to a new DataFrame](#unpack-a-dataframe-s-json-column-to-a-new-dataframe)
      * [Query a JSON column](#query-a-json-column)
   * [Sorting and Searching](#sorting-and-searching)
      * [Get distinct values of a column](#get-distinct-values-of-a-column)
      * [Filter a Dataframe based on substring search](#filter-a-dataframe-based-on-substring-search)
      * [Filter based on an IN list](#filter-based-on-an-in-list)
      * [Filter based on a NOT IN list](#filter-based-on-a-not-in-list)
      * [Filter based on a column's length](#filter-based-on-a-column-s-length)
      * [Filter based on a specific column value](#filter-based-on-a-specific-column-value)
      * [Sort DataFrame by a column](#sort-dataframe-by-a-column)
      * [Take the first N rows of a DataFrame](#take-the-first-n-rows-of-a-dataframe)
      * [Multiple filter conditions](#multiple-filter-conditions)
      * [Remove duplicates](#remove-duplicates)
      * [Filter a column](#filter-a-column)
   * [Grouping](#grouping)
      * [Get the maximum of a column](#get-the-maximum-of-a-column)
      * [Group by then filter on the count](#group-by-then-filter-on-the-count)
      * [Find the top N per row group (use N=1 for maximum)](#find-the-top-n-per-row-group-use-n-1-for-maximum)
      * [Count unique after grouping](#count-unique-after-grouping)
      * [Sum a list of columns](#sum-a-list-of-columns)
      * [Aggregate all numeric columns](#aggregate-all-numeric-columns)
      * [Sum a column](#sum-a-column)
      * [Compute a histogram](#compute-a-histogram)
      * [Group key/values into a list](#group-key-values-into-a-list)
      * [Group and sort](#group-and-sort)
      * [Group by multiple columns](#group-by-multiple-columns)
      * [Aggregate multiple columns](#aggregate-multiple-columns)
      * [Aggregate multiple columns](#aggregate-multiple-columns)
      * [Count distinct values on all columns](#count-distinct-values-on-all-columns)
   * [Joining DataFrames](#joining-dataframes)
      * [Concatenate two DataFrames](#concatenate-two-dataframes)
      * [Join two DataFrames by column name](#join-two-dataframes-by-column-name)
      * [Join two DataFrames with an expression](#join-two-dataframes-with-an-expression)
      * [Load multiple files into a single DataFrame](#load-multiple-files-into-a-single-dataframe)
      * [Multiple join conditions](#multiple-join-conditions)
      * [Subtract DataFrames](#subtract-dataframes)
      * [Various Spark join types](#various-spark-join-types)
   * [Handling Missing Data](#handling-missing-data)
      * [Filter rows with None or Null values](#filter-rows-with-none-or-null-values)
      * [Drop rows with Null values](#drop-rows-with-null-values)
      * [Count all Null or NaN values in a DataFrame](#count-all-null-or-nan-values-in-a-dataframe)
   * [Pandas](#pandas)
      * [Convert Spark DataFrame to Pandas DataFrame](#convert-spark-dataframe-to-pandas-dataframe)
      * [Convert N rows from a DataFrame to a Pandas DataFrame](#convert-n-rows-from-a-dataframe-to-a-pandas-dataframe)
      * [Define a UDAF with Pandas](#define-a-udaf-with-pandas)
      * [Define a Pandas Grouped Map Function](#define-a-pandas-grouped-map-function)
   * [Performance](#performance)
      * [Change DataFrame partitioning](#change-dataframe-partitioning)
      * [Coalesce DataFrame partitions](#coalesce-dataframe-partitions)
      * [Increase Spark driver/executor heap space](#increase-spark-driver-executor-heap-space)
<!--te-->
    

Loading and Saving Data
=======================
Loading data into DataFrames from various formats, and saving it out again.

Save a DataFrame in Parquet format
----------------------------------

```python
df.write.parquet("output.parquet")
```


Load a DataFrame from CSV
-------------------------

```python
df = spark.read.format("csv").option("header", True).load("auto-mpg.csv")
```


Save a DataFrame in CSV format
------------------------------

```python
df.write.csv("output.csv")
```


Save a DataFrame in a single CSV file
-------------------------------------

```python
df.coalesce(1).write.csv("single.csv")
```


Save a DataFrame to CSV with a header
-------------------------------------

```python
df.coalesce(1).write.csv("header.csv", header="true")
```


Save a DataFrame to CSV, overwriting existing data
--------------------------------------------------

```python
df.write.mode("overwrite").csv("output.csv")
```


Provide the schema when loading a DataFrame from CSV
----------------------------------------------------

```python
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
    .load("auto-mpg.csv")
)
```


Save DataFrame as a dynamic partitioned table
---------------------------------------------

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("append").partitionBy("modelyear").saveAsTable("autompg")
```


Read a CSV file from OCI Object Storage
---------------------------------------

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
PATH = "oci://your_path_here"
df = spark.read.format("csv").option("header", True).load(PATH)
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

Modify a DataFrame column
-------------------------

```python
from pyspark.sql.functions import col, concat, lit

df = df.withColumn("modelyear", concat(lit("19"), col("modelyear")))
```


Add a new column with builtin UDF
---------------------------------

```python
from pyspark.sql.functions import upper, lower

df = df.withColumn("upper", upper(df.carname)).withColumn(
    "lower", lower(df.carname)
)
```


Create a custom UDF
-------------------

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

first_word_udf = udf(lambda x: x.split()[0], StringType())
df = df.withColumn("manufacturer", first_word_udf(df.carname))
```


Concatenate columns
-------------------

```python
from pyspark.sql.functions import concat, col, lit

df = df.withColumn(
    "concatenated", concat(col("cylinders"), lit("_"), col("mpg"))
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


Convert an RDD to Data Frame
----------------------------

```python
from pyspark.sql import Row

row = Row("val")
df = rdd.map(row).toDF()
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


Drop a column
-------------

```python
df = df.drop("horsepower")
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


Select particular columns from a DataFrame
------------------------------------------

```python
df = df.select(["mpg", "cylinders", "displacement"])
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


Transforming Data
=================
Data conversions and other modifications.

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

Get distinct values of a column
-------------------------------

```python
distinct = df.select("cylinders").distinct()
```


Filter a Dataframe based on substring search
--------------------------------------------

```python
from pyspark.sql.functions import col

filtered = df.where(col("carname").like("%custom%"))
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


Filter based on a column's length
---------------------------------

```python
from pyspark.sql.functions import col, length

filtered = df.where(length(col("carname")) < 12)
```


Filter based on a specific column value
---------------------------------------

```python
from pyspark.sql.functions import col

filtered = df.where(col("cylinders") == "8")
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


Multiple filter conditions
--------------------------

```python
from pyspark.sql.functions import col

or_conditions = df.filter((col("mpg") > "30") | (col("acceleration") < "10"))
and_conditions = df.filter((col("mpg") > "30") & (col("acceleration") < "13"))
```


Remove duplicates
-----------------

```python
filtered = df.dropDuplicates(["carname"])
```


Filter a column
---------------

```python
from pyspark.sql.functions import col

filtered = df.filter(col("mpg") > "30")
```


Grouping
========
Group DataFrame data by key to perform aggregats like sums, averages, etc.

Get the maximum of a column
---------------------------

```python
from pyspark.sql.functions import col, max

grouped = df.select(max(col("horsepower")).alias("max_horsepower"))
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

n = 5
w = Window().partitionBy("cylinders").orderBy(col("horsepower").desc())
result = (
    df.withColumn("horsepower", col("horsepower").cast("double"))
    .withColumn("rn", row_number().over(w))
    .where(col("rn") <= n)
    .select("*")
)
```


Count unique after grouping
---------------------------

```python
from pyspark.sql.functions import countDistinct, udf
from pyspark.sql.types import StringType

manufacturer_udf = udf(lambda x: x.split()[0], StringType())
df = df.withColumn("manufacturer", manufacturer_udf(df.carname))
grouped = df.groupBy("manufacturer").agg(countDistinct("cylinders"))
```


Sum a list of columns
---------------------

```python
exprs = {x: "sum" for x in ("weight", "cylinders", "mpg")}
summed = df.agg(exprs)
```


Aggregate all numeric columns
-----------------------------

```python
numerics = set(["decimal", "double", "float", "integer", "long", "short"])
exprs = {x[0]: "sum" for x in df.dtypes if x[1] in numerics}
summed = df.agg(exprs)
```


Sum a column
------------

```python
from pyspark.sql.functions import sum, udf
from pyspark.sql.types import StringType

manufacturer_udf = udf(lambda x: x.split()[0], StringType())
df = df.withColumn("manufacturer", manufacturer_udf(df.carname))
grouped = df.groupBy("manufacturer").agg(sum("weight").alias("total_weight"))
```


Compute a histogram
-------------------

```python
assert False, ""
```


Group key/values into a list
----------------------------

```python
from pyspark.sql.functions import col, collect_list, udf
from pyspark.sql.types import StringType

manufacturer_udf = udf(lambda x: x.split()[0], StringType())
df = df.withColumn("manufacturer", manufacturer_udf(df.carname))
collected = df.groupBy("manufacturer").agg(
    collect_list(col("carname")).alias("models")
)
```


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
from pyspark.sql.functions import avg, desc, udf
from pyspark.sql.types import StringType

manufacturer_udf = udf(lambda x: x.split()[0], StringType())
df = df.withColumn("manufacturer", manufacturer_udf(df.carname))
grouped = (
    df.groupBy(["manufacturer", "cylinders"])
    .agg(avg("horsepower").alias("avg_horsepower"))
    .orderBy(desc("avg_horsepower"))
)
```


Aggregate multiple columns
--------------------------

```python
from pyspark.sql.functions import asc, desc_nulls_last, udf
from pyspark.sql.types import StringType

manufacturer_udf = udf(lambda x: x.split()[0], StringType())
df = df.withColumn("manufacturer", manufacturer_udf(df.carname))
expressions = dict(horsepower="avg", weight="max", displacement="max")
grouped = df.groupBy("manufacturer").agg(expressions)
```


Aggregate multiple columns
--------------------------

```python
from pyspark.sql.functions import asc, desc_nulls_last, udf
from pyspark.sql.types import StringType

manufacturer_udf = udf(lambda x: x.split()[0], StringType())
df = df.withColumn("manufacturer", manufacturer_udf(df.carname))
expressions = dict(horsepower="avg", weight="max", displacement="max")
orderings = [
    desc_nulls_last("max(displacement)"),
    desc_nulls_last("avg(horsepower)"),
    asc("max(weight)"),
]
grouped = df.groupBy("manufacturer").agg(expressions).orderBy(*orderings)
```


Count distinct values on all columns
------------------------------------

```python
from pyspark.sql.functions import countDistinct

grouped = df.agg(*(countDistinct(c) for c in df.columns))
```


Joining DataFrames
==================
Joining and stacking DataFrames.

Concatenate two DataFrames
--------------------------

```python
df1 = spark.read.format("csv").option("header", True).load("part1.csv")
df2 = spark.read.format("csv").option("header", True).load("part2.csv")
df = df1.union(df2)
```


Join two DataFrames by column name
----------------------------------

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

first_word_udf = udf(lambda x: x.split()[0], StringType())
df = df.withColumn("manufacturer", first_word_udf(df.carname))
countries = (
    spark.read.format("csv").option("header", True).load("manufacturers.csv")
)
joined = df.join(countries, "manufacturer")
```


Join two DataFrames with an expression
--------------------------------------

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

first_word_udf = udf(lambda x: x.split()[0], StringType())
df = df.withColumn("manufacturer", first_word_udf(df.carname))
countries = (
    spark.read.format("csv").option("header", True).load("manufacturers.csv")
)
joined = df.join(countries, df.manufacturer == countries.manufacturer)
```


Load multiple files into a single DataFrame
-------------------------------------------

```python
# Approach 1: Use a list.
df = (
    spark.read.format("csv")
    .option("header", True)
    .load(["part1.csv", "part2.csv"])
)

# Approach 2: Use a wildcard.
df = spark.read.format("csv").option("header", True).load("part*.csv")
```


Multiple join conditions
------------------------

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

first_word_udf = udf(lambda x: x.split()[0], StringType())
df = df.withColumn("manufacturer", first_word_udf(df.carname))
countries = (
    spark.read.format("csv").option("header", True).load("manufacturers.csv")
)
joined = df.join(
    countries,
    (df.manufacturer == countries.manufacturer)
    | (df.mpg == countries.manufacturer),
)
```


Subtract DataFrames
-------------------

```python
from pyspark.sql.functions import col

reduced = df.subtract(df.where(col("mpg") < "25"))
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


Performance
===========
A few performance tips and tricks.

Change DataFrame partitioning
-----------------------------

```python
from pyspark.sql.functions import col

df = df.repartition(col("modelyear"))
number_of_partitions = 5
df = df.repartitionByRange(number_of_partitions, col("mpg"))
```


Coalesce DataFrame partitions
-----------------------------

```python
import math

target_partitions = math.ceil(df.rdd.getNumPartitions() / 2)
df = df.coalesce(target_partitions)
```


Increase Spark driver/executor heap space
-----------------------------------------

```python

```
