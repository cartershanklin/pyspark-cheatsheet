import os

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col

from delta import *

# Create our Spark session and SQL Context.
warehouse_path = "file://{}/spark_warehouse".format(os.getcwd())
builder = (
    SparkSession.builder.master("local[*]")
    .config("spark.executor.memory", "2G")
    .config("spark.driver.memory", "2G")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.warehouse.dir", warehouse_path)
    .appName("cheatsheet")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
sqlContext = SQLContext(spark)

# Unmodified Auto dataset.
auto_df = spark.read.format("csv").option("header", True).load("data/auto-mpg.csv")

# Fixed Auto dataset.
auto_df_fixed = spark.read.format("csv").option("header", True).load("data/auto-mpg-fixed.csv")
for (column_name) in ("mpg cylinders displacement horsepower weight acceleration".split()):
    auto_df_fixed = auto_df_fixed.withColumn(column_name, col(column_name).cast("double"))
auto_df_fixed = auto_df_fixed.withColumn("modelyear", col("modelyear").cast("int"))
auto_df_fixed = auto_df_fixed.withColumn("origin", col("origin").cast("int"))

# Cover type dataset.
covtype_df = spark.read.format("parquet").load("data/covtype.parquet")
for column_name in covtype_df.columns:
    covtype_df = covtype_df.withColumn(column_name, col(column_name).cast("int"))

# Customer spend dataset.
spend_df = spark.read.format("csv").option("header", True).load("data/customer_spend.csv")

# Weblog.
weblog_df = spark.read.format("csv").option("header", True).load("data/weblog.csv")
