# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession


def session_spark():

    spark = SparkSession \
        .builder \
        .appName("Divvy Bikes") \
        .enableHiveSupport() \
        .getOrCreate()

    return spark


def save_hdfs(logger, dataframe, path):

        logger.info("Salvando dataframe no HDFS")

        dataframe.coalesce(10)\
                    .write \
                    .mode("overwrite")\
                    .option("compression", "snappy")\
                    .parquet(path)


def quiet_logs(spark):

    log4jLogger = spark._jvm.org.apache.log4j
    log4jLogger.LogManager.getLogger("org").setLevel(log4jLogger.Level.OFF)
    log4jLogger.LogManager.getLogger("akka").setLevel(log4jLogger.Level.OFF)
    log4jLogger.LogManager.getLogger("hive").setLevel(log4jLogger.Level.OFF)

    return log4jLogger
