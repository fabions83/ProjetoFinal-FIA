# -*- coding: utf-8 -*-

from collections import OrderedDict

from utils.openweathermap_utils import get_weather
from pyspark.sql import Row
from pyspark.sql.functions import col, struct, collect_list, mean, round
from pyspark.ml import PipelineModel


class PredictDataframe():

    def __init__(self, spark):
        self.spark = spark

    @staticmethod
    def convert_to_row(d):
        return Row(**OrderedDict(sorted(d.items())))

    def get_data(self):

        temperature_list = []
        weather_json = get_weather()
        for item in weather_json['list']:
            temperature_list.append({'datetime': item['dt_txt'],
                              'humidity': float(item['main']['humidity']),
                              'pressure': float(item['main']['pressure']),
                              'temperature': float(item['main']['temp']),
                              'weather_description': item['weather'][0]['description'],
                              'wind_speed': float(item['wind']['speed'])})

        temperature = self.spark.sparkContext.parallelize(temperature_list)\
            .map(self.convert_to_row) \
            .toDF()

        return temperature

    def join_dataframe(self, bikes, temperature):

        stations = bikes.select("from_station_id", "latitude", "longitude",
                                "mean_dpcapacity_start", "mean_dpcapacity_end",
                                "sum_subscriber", "sum_customer")\
                        .distinct()

        dates = temperature.select("datetime").distinct()

        bikes_df = dates.crossJoin(stations)\
                        .withColumnRenamed("datetime", "starttime") \
                        .withColumn("starttime", col("starttime").cast("timestamp"))

        dataframe = bikes_df.join(temperature, bikes_df.starttime == temperature.datetime) \
                            .withColumn("from_station_id", col("from_station_id").cast("int"))

        return dataframe

    def get_model(self):

        return PipelineModel.load("/user/labdata/model")

    def create_json(self, predictions):

        group_dates = predictions.groupBy("from_station_id", "latitude",
                                            "longitude", "date",
                                          "mean_dpcapacity_start", "mean_dpcapacity_end",
                                          "sum_subscriber",
                                          "sum_customer")\
                    .agg(collect_list(struct(col("part_time"),
                        col("prediction").cast("int").alias("bicycle_rentals"),
                        col("weather_description_mf"), col("week_days"),
                        col("holiday"), col("humidity"), col("pressure"),
                        col("temperature"), col("wind_speed"))).alias("info"))

        group_info = group_dates.groupBy("from_station_id", "latitude", "longitude",
                                         "mean_dpcapacity_start", "mean_dpcapacity_end",
                                         "sum_subscriber", "sum_customer")\
                                .agg(collect_list(struct(col("date"), col("info")))\
                                .alias("dates"))

        return group_info

