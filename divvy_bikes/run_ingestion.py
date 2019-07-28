# -*- coding: utf-8 -*-

from utils.spark_utils import session_spark, quiet_logs, save_hdfs
from ingestion.bikes import BikesDataframe
from ingestion.temperature import TemperatureDataframe
from pyspark.sql import SparkSession


def main():

    spark = SparkSession \
        .builder \
        .appName("Divvy Bikes") \
        .enableHiveSupport() \
        .getOrCreate()
    # spark = session_spark()
    #spark.sparkContext.addPyFile("utils.zip")
    #spark.sparkContext.addPyFile("ingestion.zip")

    log4jLogger = quiet_logs(spark)
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.info("Iniciando Ingestão")

    logger.info("Ingestão dos dados de aluguel de bikes")
    bikes = BikesDataframe(spark)
    dataframe = bikes.create()
    save_hdfs(logger, dataframe, "hdfs:///user/labdata/bikes")

    logger.info("Ingestão dos dados de Temperatura")
    temperature = TemperatureDataframe(spark)
    dataframe = temperature.create()
    save_hdfs(logger, dataframe, "hdfs:///user/labdata/temperature")

    logger.info("Finalizando a Ingestão")


if __name__ == "__main__":
    main()
