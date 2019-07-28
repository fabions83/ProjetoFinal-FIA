# -*- coding: utf-8 -*-

from utils.spark_utils import session_spark, quiet_logs, save_hdfs
from ml.dataframe import CreateDataframe
from ml.training import TrainingDataframe
from pyspark.sql import SparkSession


def main():

    spark = SparkSession \
        .builder \
        .appName("Divvy Bikes") \
        .enableHiveSupport() \
        .getOrCreate()
    # spark = session_spark()
    # spark.sparkContext.addPyFile("utils.zip")
    # spark.sparkContext.addPyFile("ml.zip")

    log4jLogger = quiet_logs(spark)
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.info("Iniciando criação do Dataframe")

    dataframe = CreateDataframe(spark)

    logger.info("Buscando dados no HDFS")
    bikes, temperature = dataframe.get_data()

    logger.info("Join dataframe de bikes e temperatura e criando colunas adicionais")
    dataframe_join = dataframe.join_dataframe(bikes, temperature)
    dataframe_new_columns = dataframe.create_new_columns(dataframe_join)\
                            .select("starttime", "date", "from_station_id",
                                            "latitude", "longitude", "month",
                                            "mean_dpcapacity_start", "mean_dpcapacity_end",
                                            "sum_subscriber", "sum_customer","temperature",
                                            "humidity", "pressure", "wind_speed",
                                            "weather_description", "holiday",
                                            "week_days", "part_time")

    logger.info("Buscando a condição climática mais frequente por período")
    mf_condition_part_time = dataframe.get_mf_part_time(dataframe_new_columns)

    logger.info("Criando o dataframe final com a variável target")
    dataframe_final = dataframe.group_target(dataframe_new_columns, mf_condition_part_time).cache()
    dataframe_final.take(1)
    #dataframe_final.show()
    #dataframe_final.describe().show()
    #dataframe_final.coalesce(1).write.option("header", "true").csv("/user/labdata/bikes.csv")

    logger.info("Dividindo dataframe em treino e teste")
    train, test = dataframe_final.randomSplit([0.7, 0.3])

    logger.info("Treinando o modelo")
    train_dataframe = TrainingDataframe(spark)
    pipeline = train_dataframe.preprocessing()
    pipeline_model = train_dataframe.training(pipeline, train)

    logger.info("Realizando previsões")
    predictions = train_dataframe.testing(pipeline_model, test)
    #predictions.select("bicycle_rentals", "prediction").describe().show()

    logger.info("Avaliando o modelo")
    rmse = train_dataframe.evaluator(predictions)
    print("Root Mean Squared Error (RMSE) nos valores de teste = %g" % rmse)

    logger.info("Salvando o modelo")
    pipeline_model.write().overwrite().save("/user/labdata/model")


if __name__ == "__main__":
    main()
