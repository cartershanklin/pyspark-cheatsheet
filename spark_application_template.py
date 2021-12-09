import argparse

from pyspark import SparkConf
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    # Your arguments here.
    args = parser.parse_args()

    # Advanced configurations, if needed.
    conf = SparkConf()
    # conf.set("foo", "bar")

    # Get the Spark Session.
    spark = (
        SparkSession.builder.appName("myapp")
        .config(conf=conf)
        .getOrCreate()
    )

    # Less verbose logging, optional.
    spark.sparkContext.setLogLevel("WARN")

    # The rest of your Spark application here.

if __name__ == '__main__':
    main()
