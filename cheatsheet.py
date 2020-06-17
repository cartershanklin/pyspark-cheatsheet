import argparse
import datetime
import hashlib
import inspect
import os
import re
import sys
import yaml
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from slugify import slugify

spark = SparkSession.builder.appName("cheatsheet").getOrCreate()
sqlContext = SQLContext(spark)


class snippet:
    def __init__(self):
        self.dataset = None
        self.name = None
        self.hash = hashlib.md5(str(self.__class__).encode()).hexdigest()
        self.convert_numerics = False

    def load_data(self):
        assert self.dataset is not None, "Dataset not set"
        if self.dataset == "UNUSED":
            return None
        df = (
            spark.read.format("csv")
            .option("header", True)
            .load(os.path.join("data", self.dataset))
        )
        if self.convert_numerics:
            if self.dataset == "auto-mpg.csv":
                from pyspark.sql.functions import col

                for (
                    column_name
                ) in (
                    "mpg cylinders displacement horsepower weight acceleration".split()
                ):
                    df = df.withColumn(column_name, col(column_name).cast("double"))
            elif self.dataset == "customer_spend.csv":
                from pyspark.sql.functions import col, udf
                from pyspark.sql.types import DecimalType
                from decimal import Decimal
                from money_parser import price_str

                money_convert = udf(
                    lambda x: Decimal(price_str(x)) if x is not None else None,
                    DecimalType(8, 4),
                )
                df = df.withColumn(
                    "customer_id", col("customer_id").cast("integer")
                ).withColumn("spend_dollars", money_convert(df.spend_dollars))
        return df

    def snippet(self, df):
        assert False, "Snippet not overridden"

    def run(self):
        assert self.dataset is not None, "Dataset not set"
        assert self.name is not None, "Name not set"
        print("--- {} ---".format(self.name))
        self.df = self.load_data()
        retval = self.snippet(self.df)
        if retval is not None:
            retval.show()


class dfo_modify_column(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Modify a DataFrame column"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 200

    def snippet(self, df):
        from pyspark.sql.functions import col, concat, lit

        df = df.withColumn("modelyear", concat(lit("19"), col("modelyear")))
        return df


class dfo_add_column_builtin(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Add a new column with to a DataFrame"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 100

    def snippet(self, df):
        from pyspark.sql.functions import upper, lower

        df = df.withColumn("upper", upper(df.carname)).withColumn(
            "lower", lower(df.carname)
        )
        return df


class dfo_add_column_custom_udf(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Create a custom UDF"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 2100

    def snippet(self, df):
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType

        first_word_udf = udf(lambda x: x.split()[0], StringType())
        df = df.withColumn("manufacturer", first_word_udf(df.carname))
        return df


class dfo_concat_columns(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Concatenate columns"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 450

    def snippet(self, df):
        from pyspark.sql.functions import concat, col, lit

        df = df.withColumn(
            "concatenated", concat(col("cylinders"), lit("_"), col("mpg"))
        )
        return df


class dfo_string_to_double(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert String to Double"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1000

    def snippet(self, df):
        from pyspark.sql.functions import col

        df = df.withColumn("horsepower", col("horsepower").cast("double"))
        return df


class dfo_string_to_integer(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert String to Integer"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1100

    def snippet(self, df):
        from pyspark.sql.functions import col

        df = df.withColumn("horsepower", col("horsepower").cast("int"))
        return df


class dfo_change_column_name_single(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Change a column name"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 600

    def snippet(self, df):
        df = df.withColumnRenamed("horsepower", "horses")
        return df


class dfo_change_column_name_multi(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Change multiple column names"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 700

    def snippet(self, df):
        df = df.withColumnRenamed("horsepower", "horses").withColumnRenamed(
            "modelyear", "year"
        )
        return df


class dfo_dataframe_from_rdd(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert an RDD to Data Frame"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1500

    def load_data(self):
        return spark.sparkContext.textFile(self.dataset)

    def snippet(self, rdd):
        from pyspark.sql import Row

        row = Row("val")
        df = rdd.map(row).toDF()
        return df


class dfo_empty_dataframe(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Create an empty dataframe with a specified schema"
        self.category = "DataFrame Operations"
        self.dataset = "NA"
        self.priority = 900

    def load_data(self):
        pass

    def snippet(self, rdd):
        from pyspark.sql.types import StructField, StructType, LongType, StringType

        schema = StructType(
            [
                StructField("my_id", LongType(), True),
                StructField("my_string", StringType(), True),
            ]
        )
        df = spark.createDataFrame([], schema)
        return df


class dfo_drop_column(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Drop a column"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 500

    def snippet(self, df):
        df = df.drop("horsepower")
        return df


class dfo_print_contents_rdd(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Print the contents of an RDD"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1600

    def load_data(self):
        return spark.sparkContext.textFile(self.dataset)

    def snippet(self, rdd):
        print(rdd.take(10))


class dfo_print_contents_dataframe(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Print the contents of a DataFrame"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1700

    def snippet(self, df):
        df.show(10)


class dfo_column_conditional(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Add a column with multiple conditions"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 300

    def snippet(self, df):
        from pyspark.sql.functions import col, when

        df = df.withColumn(
            "mpg_class",
            when(col("mpg") <= 20, "low")
            .when(col("mpg") <= 30, "mid")
            .when(col("mpg") <= 40, "high")
            .otherwise("very high"),
        )
        return df


class dfo_constant_column(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Add a constant column"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 400

    def snippet(self, df):
        from pyspark.sql.functions import lit

        df = df.withColumn("one", lit(1))
        return df


class dfo_foreach(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Process each row of a DataFrame"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1800

    def snippet(self, df):
        import os

        def foreach_function(row):
            if row.horsepower is not None:
                os.system("echo " + row.horsepower)

        df.foreach(foreach_function)


class dfo_map(snippet):
    def __init__(self):
        super().__init__()
        self.name = "DataFrame Map example"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1900

    def snippet(self, df):
        def map_function(row):
            if row.horsepower is not None:
                return [float(row.horsepower) * 10]
            else:
                return [None]

        df = df.rdd.map(map_function).toDF()
        return df


class dfo_flatmap(snippet):
    def __init__(self):
        super().__init__()
        self.name = "DataFrame Flatmap example"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 2000

    def snippet(self, df):
        from pyspark.sql.types import Row

        def flatmap_function(row):
            if row.cylinders is not None:
                return list(range(int(row.cylinders)))
            else:
                return [None]

        rdd = df.rdd.flatMap(flatmap_function)
        row = Row("val")
        df = rdd.map(row).toDF()
        return df


class dfo_constant_dataframe(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Create a constant dataframe"
        self.category = "DataFrame Operations"
        self.dataset = "UNUSED"
        self.priority = 950

    def snippet(self, df):
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
        return df


class dfo_select_particular(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Select particular columns from a DataFrame"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 800

    def snippet(self, df):
        df = df.select(["mpg", "cylinders", "displacement"])
        return df


class dfo_size(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Get the size of a DataFrame"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1200

    def snippet(self, df):
        print("{} rows".format(df.count()))
        print("{} columns".format(len(df.columns)))


class dfo_get_number_partitions(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Get a DataFrame's number of partitions"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1300

    def snippet(self, df):
        print("{} partition(s)".format(df.rdd.getNumPartitions()))


class dfo_get_dtypes(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Get data types of a DataFrame's columns"
        self.category = "DataFrame Operations"
        self.dataset = "auto-mpg.csv"
        self.priority = 1400

    def snippet(self, df):
        print(df.dtypes)


class group_max_value(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Get the maximum of a column"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 500

    def snippet(self, df):
        from pyspark.sql.functions import col, max

        grouped = df.select(max(col("horsepower")).alias("max_horsepower"))
        return grouped


class group_filter_on_count(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Group by then filter on the count"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 1100

    def snippet(self, df):
        from pyspark.sql.functions import col

        grouped = df.groupBy("cylinders").count().where(col("count") > 100)
        return grouped


class group_topn_per_group(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Find the top N per row group (use N=1 for maximum)"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 1200

    def snippet(self, df):
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
        return result


class group_count_unique_after_group(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Count unique after grouping"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 900

    def snippet(self, df):
        from pyspark.sql.functions import countDistinct

        grouped = df.groupBy("cylinders").agg(countDistinct("mpg"))
        return grouped


class group_sum_column(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Sum a column"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 700

    def snippet(self, df):
        from pyspark.sql.functions import sum

        grouped = df.groupBy("cylinders").agg(sum("weight").alias("total_weight"))
        return grouped


class group_sum_columns_no_group(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Sum a list of columns"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 600

    def snippet(self, df):
        exprs = {x: "sum" for x in ("weight", "cylinders", "mpg")}
        summed = df.agg(exprs)
        return summed


class group_histogram(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Compute a histogram"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 1400

    def snippet(self, df):
        from pyspark.sql.functions import col

        # Target column must be numeric.
        df = df.withColumn("horsepower", col("horsepower").cast("double"))

        # N is the number of bins.
        N = 11
        histogram = df.select("horsepower").rdd.flatMap(lambda x: x).histogram(N)


class group_key_value_to_key_list(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Group key/values into a list"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 1300

    def snippet(self, df):
        from pyspark.sql.functions import col, collect_list

        collected = df.groupBy("cylinders").agg(
            collect_list(col("carname")).alias("models")
        )
        return collected


class group_group_and_sort(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Group and sort"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 100

    def snippet(self, df):
        from pyspark.sql.functions import avg, desc

        grouped = (
            df.groupBy("cylinders")
            .agg(avg("horsepower").alias("avg_horsepower"))
            .orderBy(desc("avg_horsepower"))
        )
        return grouped


class group_multiple_columns(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Group by multiple columns"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 200

    def snippet(self, df):
        from pyspark.sql.functions import avg, desc

        grouped = (
            df.groupBy(["modelyear", "cylinders"])
            .agg(avg("horsepower").alias("avg_horsepower"))
            .orderBy(desc("avg_horsepower"))
        )
        return grouped


class group_agg_multiple_columns(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Aggregate multiple columns"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 300

    def snippet(self, df):
        from pyspark.sql.functions import asc, desc_nulls_last

        expressions = dict(horsepower="avg", weight="max", displacement="max")
        grouped = df.groupBy("modelyear").agg(expressions)
        return grouped


class group_order_multiple_columns(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Aggregate multiple columns with custom orderings"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 400

    def snippet(self, df):
        from pyspark.sql.functions import asc, desc_nulls_last

        expressions = dict(horsepower="avg", weight="max", displacement="max")
        orderings = [
            desc_nulls_last("max(displacement)"),
            desc_nulls_last("avg(horsepower)"),
            asc("max(weight)"),
        ]
        grouped = df.groupBy("modelyear").agg(expressions).orderBy(*orderings)
        return grouped


class group_distinct_all_columns(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Count distinct values on all columns"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 1000

    def snippet(self, df):
        from pyspark.sql.functions import countDistinct

        grouped = df.agg(*(countDistinct(c) for c in df.columns))
        return grouped


class group_aggregate_all_numerics(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Aggregate all numeric columns"
        self.category = "Grouping"
        self.dataset = "auto-mpg.csv"
        self.priority = 800

    def snippet(self, df):
        numerics = set(["decimal", "double", "float", "integer", "long", "short"])
        exprs = {x[0]: "sum" for x in df.dtypes if x[1] in numerics}
        summed = df.agg(exprs)
        return summed


class sortsearch_distinct_values(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Get distinct values of a column"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 1000

    def snippet(self, df):
        distinct = df.select("cylinders").distinct()
        return distinct


class sortsearch_string_contents(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Filter a Dataframe based on substring search"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 500

    def snippet(self, df):
        from pyspark.sql.functions import col

        filtered = df.where(col("carname").like("%custom%"))
        return filtered


class sortsearch_in_list(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Filter based on an IN list"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 300

    def snippet(self, df):
        from pyspark.sql.functions import col

        filtered = df.where(col("cylinders").isin(["4", "6"]))
        return filtered


class sortsearch_not_in_list(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Filter based on a NOT IN list"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 400

    def snippet(self, df):
        from pyspark.sql.functions import col

        filtered = df.where(~col("cylinders").isin(["4", "6"]))
        return filtered


class sortsearch_column_length(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Filter based on a column's length"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 600

    def snippet(self, df):
        from pyspark.sql.functions import col, length

        filtered = df.where(length(col("carname")) < 12)
        return filtered


class sortsearch_equality(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Filter based on a specific column value"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 200

    def snippet(self, df):
        from pyspark.sql.functions import col

        filtered = df.where(col("cylinders") == "8")
        return filtered


class sortsearch_sort_descending(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Sort DataFrame by a column"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 800

    def snippet(self, df):
        from pyspark.sql.functions import col

        ascending = df.orderBy("carname")
        descending = df.orderBy(col("carname").desc())
        return descending


class sortsearch_first_1k_rows(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Take the first N rows of a DataFrame"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 900

    def snippet(self, df):
        n = 10
        reduced = df.limit(n)
        return reduced


class sortsearch_multi_filter(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Multiple filter conditions"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 700

    def snippet(self, df):
        from pyspark.sql.functions import col

        or_conditions = df.filter((col("mpg") > "30") | (col("acceleration") < "10"))
        and_conditions = df.filter((col("mpg") > "30") & (col("acceleration") < "13"))
        return and_conditions


class sortsearch_remove_duplicates(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Remove duplicates"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 1100

    def snippet(self, df):
        filtered = df.dropDuplicates(["carname"])
        return filtered


class sortsearch_filtering_basic(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Filter a column using a condition"
        self.category = "Sorting and Searching"
        self.dataset = "auto-mpg.csv"
        self.priority = 100

    def snippet(self, df):
        from pyspark.sql.functions import col

        filtered = df.filter(col("mpg") > "30")
        return filtered


class transform_string_to_date(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert string to date"
        self.category = "Transforming Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 400

    def snippet(self, df):
        from pyspark.sql.functions import col

        df = spark.sparkContext.parallelize([["2021-01-01"], ["2022-01-01"]]).toDF(
            ["date_col"]
        )
        df = df.withColumn("date_col", col("date_col").cast("date"))
        return df


class transform_string_to_date_custom(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert string to date with custom format"
        self.category = "Transforming Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 500

    def snippet(self, df):
        from pyspark.sql.functions import col, to_date

        df = spark.sparkContext.parallelize([["20210101"], ["20220101"]]).toDF(
            ["date_col"]
        )
        df = df.withColumn("date_col", to_date(col("date_col"), "yyyyddMM"))
        return df


class transform_unix_timestamp_to_date(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert UNIX (seconds since epoch) timestamp to date"
        self.category = "Transforming Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 600

    def snippet(self, df):
        from pyspark.sql.functions import col, from_unixtime

        df = spark.sparkContext.parallelize([["1590183026"], ["2000000000"]]).toDF(
            ["ts_col"]
        )
        df = df.withColumn("date_col", from_unixtime(col("ts_col")))
        return df


class loadsave_overwrite_specific_partitions(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Overwrite specific partitions"
        self.category = "Loading and Saving Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 900

    def snippet(self, df):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        data.write.mode("overwrite").insertInto("my_table")


class transform_fillna_specific_columns(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Fill NULL values in specific columns"
        self.category = "Transforming Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 100

    def snippet(self, df):
        df.fillna({"horsepower": 0})
        return df


class transform_fillna_col_avg(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Fill NULL values with column average"
        self.category = "Transforming Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 200

    def snippet(self, df):
        from pyspark.sql.functions import avg

        df.fillna({"horsepower": df.agg(avg("horsepower")).first()[0]})
        return df


class transform_fillna_group_avg(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Fill NULL values with group average"
        self.category = "Transforming Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 300

    def snippet(self, df):
        from pyspark.sql.functions import avg, coalesce

        unmodified_columns = df.columns
        unmodified_columns.remove("horsepower")
        manufacturer_avg = df.groupBy("cylinders").agg({"horsepower": "avg"})
        df = df.join(manufacturer_avg, "cylinders").select(
            *unmodified_columns,
            coalesce("horsepower", "avg(horsepower)").alias("horsepower")
        )
        return df


class transform_json_to_key_value(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Unpack a DataFrame's JSON column to a new DataFrame"
        self.category = "Transforming Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 700

    def snippet(self, df):
        from pyspark.sql.functions import col, explode, from_json, json_tuple

        source = spark.sparkContext.parallelize(
            [["1", '{ "a" : 10, "b" : 11 }'], ["2", '{ "a" : 20, "b" : 21 }']]
        ).toDF(["id", "json"])
        df = source.select("id", json_tuple(col("json"), "a", "b"))
        return df


class transform_query_json_column(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Query a JSON column"
        self.category = "Transforming Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 800

    def snippet(self, df):
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
        return filtered


class join_concatenate(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Concatenate two DataFrames"
        self.category = "Joining DataFrames"
        self.dataset = "auto-mpg.csv"
        self.priority = 500

    def snippet(self, df):
        df1 = spark.read.format("csv").option("header", True).load("data/part1.csv")
        df2 = spark.read.format("csv").option("header", True).load("data/part2.csv")
        df = df1.union(df2)
        return df


class join_basic(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Join two DataFrames by column name"
        self.category = "Joining DataFrames"
        self.dataset = "auto-mpg.csv"
        self.priority = 100

    def snippet(self, df):
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
        return joined


class join_basic2(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Join two DataFrames with an expression"
        self.category = "Joining DataFrames"
        self.dataset = "auto-mpg.csv"
        self.priority = 200

    def snippet(self, df):
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
        return joined


class join_multiple_files_single_dataframe(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load multiple files into a single DataFrame"
        self.category = "Joining DataFrames"
        self.dataset = "auto-mpg.csv"
        self.priority = 600

    def snippet(self, df):
        # Approach 1: Use a list.
        df = (
            spark.read.format("csv")
            .option("header", True)
            .load(["data/part1.csv", "data/part2.csv"])
        )

        # Approach 2: Use a wildcard.
        df = spark.read.format("csv").option("header", True).load("data/part*.csv")
        return df


class join_multiple_conditions(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Multiple join conditions"
        self.category = "Joining DataFrames"
        self.dataset = "auto-mpg.csv"
        self.priority = 300

    def snippet(self, df):
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
        return joined


class join_subtract_dataframes(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Subtract DataFrames"
        self.category = "Joining DataFrames"
        self.dataset = "auto-mpg.csv"
        self.priority = 700

    def snippet(self, df):
        from pyspark.sql.functions import col

        reduced = df.subtract(df.where(col("mpg") < "25"))
        return reduced


class join_different_types(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Various Spark join types"
        self.category = "Joining DataFrames"
        self.dataset = "auto-mpg.csv"
        self.priority = 400

    def snippet(self, df):
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
        return joined


class loadsave_to_parquet(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Save a DataFrame in Parquet format"
        self.category = "Loading and Saving Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 400

    def snippet(self, df):
        df.write.parquet("output.parquet")


class loadsave_dataframe_from_csv(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a DataFrame from CSV"
        self.category = "Loading and Saving Data"
        self.dataset = "UNUSED"
        self.priority = 100

    def snippet(self, df):
        # See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html
        # for a list of supported options.
        df = spark.read.format("csv").option("header", True).load("data/auto-mpg.csv")
        return df


class loadsave_dataframe_from_csv_delimiter(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a DataFrame from a Tab Separated Value (TSV) file"
        self.category = "Loading and Saving Data"
        self.dataset = "UNUSED"
        self.priority = 110

    def snippet(self, df):
        # See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html
        # for a list of supported options.
        df = (
            spark.read.format("csv")
            .option("header", True)
            .option("sep", "\t")
            .load("data/auto-mpg.tsv")
        )
        return df


class loadsave_money(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a CSV file with a money column into a DataFrame"
        self.category = "Loading and Saving Data"
        self.dataset = "UNUSED"
        self.priority = 120

    def snippet(self, df):
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
        return money2


class loadsave_date(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load a CSV file with complex dates into a DataFrame"
        self.category = "Loading and Saving Data"
        self.dataset = "UNUSED"
        self.priority = 130

    def snippet(self, df):
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
        return df


class loadsave_export_to_csv(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Save a DataFrame in CSV format"
        self.category = "Loading and Saving Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 500

    def snippet(self, df):
        # See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameWriter.html
        # for a list of supported options.
        df.write.csv("output.csv")


class loadsave_single_output_file(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Save a DataFrame in a single CSV file"
        self.category = "Loading and Saving Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 700

    def snippet(self, df):
        df.coalesce(1).write.csv("single.csv")


class loadsave_csv_with_header(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Save a DataFrame to CSV with a header"
        self.category = "Loading and Saving Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 600

    def snippet(self, df):
        # See https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameWriter.html
        # for a list of supported options.
        df.coalesce(1).write.csv("header.csv", header="true")


class loadsave_overwrite_output_directory(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Save a DataFrame to CSV, overwriting existing data"
        self.category = "Loading and Saving Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 501

    def snippet(self, df):
        df.write.mode("overwrite").csv("output.csv")


class loadsave_dataframe_from_csv_provide_schema(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Provide the schema when loading a DataFrame from CSV"
        self.category = "Loading and Saving Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 200

    def snippet(self, df):
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
        return df


class loadsave_dynamic_partitioning(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Save DataFrame as a dynamic partitioned table"
        self.category = "Loading and Saving Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 800

    def snippet(self, df):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        df.write.mode("append").partitionBy("modelyear").saveAsTable("autompg")


class loadsave_read_from_oci(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Configure security to read a CSV file from Oracle Cloud Infrastructure Object Storage"
        self.category = "Loading and Saving Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 300

    def snippet(self, df):
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


class missing_filter_none_value(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Filter rows with None or Null values"
        self.category = "Handling Missing Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 100

    def snippet(self, df):
        from pyspark.sql.functions import col

        filtered = df.where(col("horsepower").isNull())
        filtered = df.where(col("horsepower").isNotNull())
        return filtered


class missing_filter_null_value(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Drop rows with Null values"
        self.category = "Handling Missing Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 200

    def snippet(self, df):
        # thresh controls the number of nulls before the row gets dropped.
        # subset controls the columns to consider.
        df = df.na.drop(thresh=2, subset=("horsepower",))


class missing_count_of_null_nan(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Count all Null or NaN values in a DataFrame"
        self.category = "Handling Missing Data"
        self.dataset = "auto-mpg.csv"
        self.priority = 300

    def snippet(self, df):
        from pyspark.sql.functions import col, count, isnan, when

        result = df.select([count(when(isnan(c), c)).alias(c) for c in df.columns])
        result = df.select(
            [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
        )
        return result


class performance_partitioning(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Change DataFrame partitioning"
        self.category = "Performance"
        self.dataset = "auto-mpg.csv"
        self.priority = 200

    def snippet(self, df):
        from pyspark.sql.functions import col

        df = df.repartition(col("modelyear"))
        number_of_partitions = 5
        df = df.repartitionByRange(number_of_partitions, col("mpg"))
        return df


class performance_reduce_dataframe_partitions(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Coalesce DataFrame partitions"
        self.category = "Performance"
        self.dataset = "auto-mpg.csv"
        self.priority = 100

    def snippet(self, df):
        import math

        target_partitions = math.ceil(df.rdd.getNumPartitions() / 2)
        df = df.coalesce(target_partitions)
        return df


class performance_increase_heap_space(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Increase Spark driver/executor heap space"
        self.category = "Performance"
        self.dataset = "auto-mpg.csv"
        self.priority = 300

    def snippet(self, df):
        # Memory configuration depends entirely on your runtime.
        # In OCI Data Flow you control memory by selecting a larger or smaller VM.
        # No other configuration is needed.
        #
        # For other environments see the Spark "Cluster Mode Overview" to get started.
        # https://spark.apache.org/docs/latest/cluster-overview.html
        # And good luck!
        return df


class pandas_spark_dataframe_to_pandas_dataframe(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert Spark DataFrame to Pandas DataFrame"
        self.category = "Pandas"
        self.dataset = "auto-mpg.csv"
        self.priority = 100

    def snippet(self, df):
        pdf = df.toPandas()
        return df


class pandas_udaf(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Define a UDAF with Pandas"
        self.category = "Pandas"
        self.priority = 300
        self.dataset = "auto-mpg.csv"

    def snippet(self, df):
        from pyspark.sql.functions import pandas_udf, PandasUDFType
        from pyspark.sql.functions import col

        @pandas_udf("double", PandasUDFType.GROUPED_AGG)
        def mean_udaf(pdf):
            return pdf.mean()

        df = df.withColumn("mpg", col("mpg").cast("double"))
        df = df.groupby("cylinders").agg(mean_udaf(df["mpg"]))
        return df


class pandas_rescale_column(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Define a Pandas Grouped Map Function"
        self.category = "Pandas"
        self.dataset = "auto-mpg.csv"
        self.priority = 400

    def snippet(self, df):
        from pyspark.sql.functions import pandas_udf, PandasUDFType
        from pyspark.sql.functions import col

        df = df.withColumn("horsepower", col("horsepower").cast("double"))

        @pandas_udf(df.schema, PandasUDFType.GROUPED_MAP)
        def rescale(pdf):
            minv = pdf.horsepower.min()
            maxv = pdf.horsepower.max() - minv
            return pdf.assign(horsepower=(pdf.horsepower - minv) / maxv * 100)

        df = df.groupby("cylinders").apply(rescale)
        return df


class pandas_n_rows_from_dataframe_to_pandas(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Convert N rows from a DataFrame to a Pandas DataFrame"
        self.category = "Pandas"
        self.dataset = "auto-mpg.csv"
        self.priority = 200

    def snippet(self, df):
        N = 10
        pdf = df.limit(N).toPandas()
        return df


class profile_number_nulls(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Compute the number of NULLs across all columns"
        self.category = "Data Profiling"
        self.dataset = "auto-mpg.csv"
        self.priority = 100

    def snippet(self, df):
        from pyspark.sql.functions import col, count, when

        result = df.select(
            [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
        )
        return result


class profile_numeric_averages(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Compute average values of all numeric columns"
        self.category = "Data Profiling"
        self.dataset = "auto-mpg.csv"
        self.priority = 200
        self.convert_numerics = True

    def snippet(self, df):
        numerics = set(["decimal", "double", "float", "integer", "long", "short"])
        exprs = {x[0]: "avg" for x in df.dtypes if x[1] in numerics}
        result = df.agg(exprs)
        return result


class profile_numeric_min(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Compute minimum values of all numeric columns"
        self.category = "Data Profiling"
        self.dataset = "auto-mpg.csv"
        self.priority = 300
        self.convert_numerics = True

    def snippet(self, df):
        numerics = set(["decimal", "double", "float", "integer", "long", "short"])
        exprs = {x[0]: "min" for x in df.dtypes if x[1] in numerics}
        result = df.agg(exprs)
        return result


class profile_numeric_max(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Compute maximum values of all numeric columns"
        self.category = "Data Profiling"
        self.dataset = "auto-mpg.csv"
        self.priority = 400
        self.convert_numerics = True

    def snippet(self, df):
        numerics = set(["decimal", "double", "float", "integer", "long", "short"])
        exprs = {x[0]: "max" for x in df.dtypes if x[1] in numerics}
        result = df.agg(exprs)
        return result


class timeseries_zero_fill(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Zero fill missing values in a timeseries"
        self.category = "Time Series"
        self.dataset = "customer_spend.csv"
        self.priority = 100
        self.convert_numerics = True

    def snippet(self, df):
        from pyspark.sql.functions import coalesce, lit

        # Use distinct values of customer and date from the dataset itself.
        # In general it's safer to use known reference tables for IDs and dates.
        filled = df.join(
            df.select("customer_id").distinct().crossJoin(df.select("date").distinct()),
            ["date", "customer_id"],
            "right",
        ).select("date", "customer_id", coalesce("spend_dollars", lit(0)))
        return filled


class timeseries_first_seen(snippet):
    def __init__(self):
        super().__init__()
        self.name = "First Time an ID is Seen"
        self.category = "Time Series"
        self.dataset = "customer_spend.csv"
        self.priority = 150
        self.convert_numerics = True

    def snippet(self, df):
        from pyspark.sql.functions import first
        from pyspark.sql.window import Window

        w = Window().partitionBy("customer_id").orderBy("date")
        df = df.withColumn("first_seen", first("date").over(w))
        return df


class timeseries_running_sum(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Running or Cumulative Sum"
        self.category = "Time Series"
        self.dataset = "customer_spend.csv"
        self.priority = 200
        self.convert_numerics = True

    def snippet(self, df):
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
        return df


class timeseries_running_average(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Running or Cumulative Average"
        self.category = "Time Series"
        self.dataset = "customer_spend.csv"
        self.priority = 300
        self.convert_numerics = True

    def snippet(self, df):
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
        return df


class fileprocessing_load_files(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load Local File Details into a DataFrame"
        self.category = "File Processing"
        self.dataset = "UNUSED"
        self.priority = 100

    def snippet(self, df):
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
        return df


class fileprocessing_load_files_oci(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Load Files from Oracle Cloud Infrastructure into a DataFrame"
        self.category = "File Processing"
        self.dataset = "UNUSED"
        self.priority = 200

    def snippet(self, df):
        # EXCLUDE
        import oci

        def get_authenticated_client(client):
            config = oci.config.from_file()
            authenticated_client = client(config)
            return authenticated_client

        object_store_client = get_authenticated_client(
            oci.object_storage.ObjectStorageClient
        )
        # INCLUDE
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
        return df


class fileprocessing_transform_images(snippet):
    def __init__(self):
        super().__init__()
        self.name = "Transform Many Images using Pillow"
        self.category = "File Processing"
        self.dataset = "UNUSED"
        self.priority = 300

    def snippet(self, df):
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


cheat_sheet = [
    pandas_spark_dataframe_to_pandas_dataframe(),
    pandas_n_rows_from_dataframe_to_pandas(),
    pandas_udaf(),
    pandas_rescale_column(),
    performance_partitioning(),
    performance_reduce_dataframe_partitions(),
    performance_increase_heap_space(),
    missing_filter_none_value(),
    missing_filter_null_value(),
    missing_count_of_null_nan(),
    loadsave_to_parquet(),
    loadsave_dataframe_from_csv(),
    loadsave_dataframe_from_csv_delimiter(),
    loadsave_export_to_csv(),
    loadsave_csv_with_header(),
    loadsave_single_output_file(),
    loadsave_money(),
    loadsave_date(),
    loadsave_overwrite_output_directory(),
    loadsave_dataframe_from_csv_provide_schema(),
    loadsave_dynamic_partitioning(),
    loadsave_read_from_oci(),
    loadsave_overwrite_specific_partitions(),
    join_concatenate(),
    join_basic(),
    join_basic2(),
    join_multiple_files_single_dataframe(),
    join_multiple_conditions(),
    join_subtract_dataframes(),
    join_different_types(),
    transform_string_to_date(),
    transform_string_to_date_custom(),
    transform_unix_timestamp_to_date(),
    transform_fillna_specific_columns(),
    transform_fillna_col_avg(),
    transform_fillna_group_avg(),
    transform_json_to_key_value(),
    transform_query_json_column(),
    sortsearch_distinct_values(),
    sortsearch_string_contents(),
    sortsearch_in_list(),
    sortsearch_not_in_list(),
    sortsearch_column_length(),
    sortsearch_equality(),
    sortsearch_sort_descending(),
    sortsearch_first_1k_rows(),
    sortsearch_multi_filter(),
    sortsearch_remove_duplicates(),
    sortsearch_filtering_basic(),
    group_max_value(),
    group_filter_on_count(),
    group_topn_per_group(),
    group_count_unique_after_group(),
    group_sum_columns_no_group(),
    group_aggregate_all_numerics(),
    group_sum_column(),
    group_histogram(),
    group_key_value_to_key_list(),
    group_group_and_sort(),
    group_multiple_columns(),
    group_agg_multiple_columns(),
    group_order_multiple_columns(),
    group_distinct_all_columns(),
    dfo_modify_column(),
    dfo_add_column_builtin(),
    dfo_add_column_custom_udf(),
    dfo_concat_columns(),
    dfo_string_to_double(),
    dfo_string_to_integer(),
    dfo_change_column_name_single(),
    dfo_change_column_name_multi(),
    dfo_dataframe_from_rdd(),
    dfo_empty_dataframe(),
    dfo_drop_column(),
    dfo_print_contents_rdd(),
    dfo_print_contents_dataframe(),
    dfo_column_conditional(),
    dfo_constant_column(),
    dfo_foreach(),
    dfo_map(),
    dfo_flatmap(),
    dfo_constant_dataframe(),
    dfo_select_particular(),
    dfo_size(),
    dfo_get_number_partitions(),
    dfo_get_dtypes(),
    profile_number_nulls(),
    profile_numeric_averages(),
    profile_numeric_min(),
    profile_numeric_max(),
    timeseries_zero_fill(),
    timeseries_first_seen(),
    timeseries_running_sum(),
    timeseries_running_average(),
    fileprocessing_load_files(),
    fileprocessing_load_files_oci(),
    fileprocessing_transform_images(),
]


def generate_cheatsheet():
    # Gather up all the categories and snippets.
    snippets = dict()
    for cheat in cheat_sheet:
        if cheat.category not in snippets:
            snippets[cheat.category] = []
        source = inspect.getsource(cheat.snippet)
        lines = source.split("\n")[1:]
        lines = [x[8:] for x in lines]
        lines = [x for x in lines if not x.startswith("return")]
        source = "\n".join(lines[:-1])
        snippets[cheat.category].append(
            (cheat.name, cheat.priority, source, cheat.hash)
        )

    # Sort by priority.
    for category, list in snippets.items():
        list = sorted(list, key=lambda x: x[1])
        snippets[category] = list

    # TOC Template
    toc_template = """
Table of contents
=================

<!--ts-->
{toc_contents}
<!--te-->
    """

    header = """

{}
{}
"""

    snippet_template = """
```python
{code}
```
"""

    # Get info about our categories.
    with open("categories.yaml") as file:
        category_spec = yaml.safe_load(file)

    sorted_categories = sorted(
        snippets.keys(), key=lambda x: category_spec[x]["priority"]
    )

    # Generate our markdown.
    toc_content_list = []
    for category in sorted_categories:
        list = snippets[category]
        category_slug = slugify(category)
        toc_content_list.append("   * [{}](#{})".format(category, category_slug))
        for name, priority, source, hash in list:
            name_slug = slugify(name)
            toc_content_list.append("      * [{}](#{})".format(name, name_slug))
    toc_contents = "\n".join(toc_content_list)

    with open("README.md", "w") as fd:
        last_updated = str(datetime.datetime.now())[:-7]
        fd.write(
            category_spec["Preamble"]["description"].format(last_updated=last_updated)
        )
        fd.write("\n")
        fd.write(toc_template.format(toc_contents=toc_contents))
        for category in sorted_categories:
            list = snippets[category]
            header_text = header.format(category, "=" * len(category))
            fd.write(header_text)
            fd.write(category_spec[category]["description"])
            toc_content_list.append("   * [{}](#{})".format(category, category_slug))
            for name, priority, source, hash in list:
                source = re.sub(r"# EXCLUDE.*# INCLUDE\n", "", source, flags=re.S)
                header_text = header.format(name, "-" * len(name))
                fd.write(header_text)
                fd.write(snippet_template.format(code=source))


def all_tests(category=None):
    for cheat in cheat_sheet:
        if category is not None and cheat.category != category:
            continue
        cheat.run()


def dump_priorities():
    for cheat in cheat_sheet:
        print("{},{},{}".format(cheat.category, cheat.name, cheat.priority))


def test(test_name):
    for cheat in cheat_sheet:
        if cheat.name == test_name:
            cheat.run()
            sys.exit(0)
    assert False, "No test named " + test_name


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all-tests", action="store_true")
    parser.add_argument("--category")
    parser.add_argument("--dump-priorities", action="store_true")
    parser.add_argument("--test")
    args = parser.parse_args()

    if args.all_tests or args.category:
        all_tests(args.category)
    elif args.dump_priorities:
        dump_priorities()
    elif args.test:
        test(args.test)
    else:
        generate_cheatsheet()


if __name__ == "__main__":
    main()
