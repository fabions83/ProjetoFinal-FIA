# -*- coding: utf-8 -*-

from utils.spark_utils import session_spark, quiet_logs, save_hdfs
from ml.predict import PredictDataframe
from ml.dataframe import CreateDataframe
from ingestion.temperature import TemperatureDataframe
from pyspark.sql.functions import col, to_json, struct
from pyspark.sql import SparkSession


def main():

    spark = SparkSession \
        .builder \
        .appName("Divvy Bikes") \
        .enableHiveSupport() \
        .getOrCreate()
    # spark = session_spark()
    # spark.sparkContext.addPyFile("utils.zip")
    # spark.sparkContext.addPyFile("ingestion.zip")
    # spark.sparkContext.addPyFile("ml.zip")

    log4jLogger = quiet_logs(spark)
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.info("Iniciando previsões")

    logger.info("Buscando dados de estações")
    train_inst = CreateDataframe(spark)
    bikes = train_inst.get_data(with_temperature=False)

    logger.info("Buscando dados de previsão do tempo")
    predict_inst = PredictDataframe(spark)
    temperature_api = predict_inst.get_data()

    temperature_inst = TemperatureDataframe(spark)
    temperature = temperature_inst.create_new_columns(temperature_api)

    logger.info("Join dataframe de estações e temperatura")
    dataframe_join = predict_inst.join_dataframe(bikes, temperature)

    logger.info("Criando dataframe final")
    dataframe_new_columns = train_inst.create_new_columns(dataframe_join)\
                                    .select("date", "from_station_id", "latitude", "month",
                                            "longitude", "mean_dpcapacity_start", "mean_dpcapacity_end",
                                            "sum_subscriber","sum_customer", "part_time", "holiday",
                                            "week_days", col("weather_condition").alias("weather_description"),
                                            "humidity", "pressure", "temperature", "wind_speed")\
                                    .cache()
    dataframe_new_columns.take(1)

    logger.info("Buscando a condição climática mais frequente por período")
    mf_condition_part_time = train_inst.get_mf_part_time(dataframe_new_columns)

    dataframe_final = train_inst.group_target(dataframe_new_columns, mf_condition_part_time) \
                                .drop("bicycle_rentals")\
                                .cache()
    dataframe_final.take(1)

    logger.info("Carregando modelo")
    model = predict_inst.get_model()

    logger.info("Realizando previsões")
    predictions = model.transform(dataframe_final)
    #predictions.show(truncate=False)

    logger.info("Criando json final")
    df_json = predict_inst.create_json(predictions)

    df_json.select(to_json(struct("*")).alias("value"))\
            .write\
            .format("kafka")\
            .option("kafka.bootstrap.servers", "quickstart.cloudera:9092")\
            .option("topic", "bikes")\
            .save()
    #df_json.coalesce(1).write.mode('overwrite').format('json').save("/user/labdata/predict")


if __name__ == "__main__":
    main()
