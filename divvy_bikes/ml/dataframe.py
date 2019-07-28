# -*- coding: utf-8 -*-

import holidays

from pyspark.sql.functions import unix_timestamp, from_unixtime, when, \
    col, date_format, udf, mean, count, round, desc, row_number, substring, concat, lit, month
from pyspark.sql.window import Window


@udf
def get_part_day(starttime):
    hour_col = starttime.hour

    if (hour_col >= 0) and (hour_col <= 6):
        return 'Early Morning'
    elif (hour_col > 6) and (hour_col <= 12):
        return 'Morning'
    elif (hour_col > 12) and (hour_col <= 18):
        return 'Afternoon'
    elif (hour_col > 18) and (hour_col <= 23):
        return 'Night'


class CreateDataframe():

    def __init__(self, spark):
        self.spark = spark

    def get_data(self, with_temperature=True):

        bikes = self.spark.read.parquet("hdfs:///user/labdata/bikes")\
                    .select("starttime", "from_station_id", "latitude", "longitude",
                            "mean_dpcapacity_start", "mean_dpcapacity_end",
                            "sum_subscriber", "sum_customer")\
                    .withColumn("starttime_join",
                                concat(substring("starttime", 0, 13), lit(":00:00")))

        if with_temperature:
            temperature = self.spark.read.parquet("hdfs:///user/labdata/temperature")

            return bikes, temperature

        return bikes

    def get_holidays(self):
        holidays_list = []

        for year in range(2013, 2020):
            holidays_list.extend(list(holidays.US(state='IL', years=year)))

        return holidays_list

    def join_dataframe(self, bikes, temperature):

        dataframe = bikes.join(temperature,
                                        bikes.starttime_join == temperature.datetime) \
            .withColumn("starttime", col("starttime").cast("timestamp")) \
            .withColumn("from_station_id", col("from_station_id").cast("int")) \
            .select("starttime", "from_station_id", "latitude",
                    "longitude", "mean_dpcapacity_start", "mean_dpcapacity_end",
                    "sum_subscriber", "sum_customer",
                    "humidity", "pressure", "temperature",
                    "weather_description", "wind_speed", )

        return dataframe

    def create_new_columns(self, dataframe_join):

        holidays_list = self.get_holidays()
        holidays_broadcast = self.spark.sparkContext.broadcast(holidays_list)

        dataframe = dataframe_join\
            .withColumn("date", from_unixtime(unix_timestamp("starttime",
                                                        "yyyy-MM-dd hh:mm:ss"), "yyyy-MM-dd")) \
            .withColumn("holiday", when(from_unixtime(unix_timestamp("starttime",
                                    "yyyy-MM-dd hh:mm:ss"), "yyyy-MM-dd")
                                    .isin(holidays_broadcast.value),"yes").otherwise("no")) \
            .withColumn("week_days", date_format("starttime", "EEEE")) \
            .withColumn("part_time", get_part_day(col("starttime"))) \
            .withColumn("month", month("date"))

        return dataframe

    def get_mf_part_time(self, dataframe):

        group = dataframe\
                .groupBy("date", "from_station_id", "part_time", "holiday", "week_days",
                        "weather_description", "month").count()
        window = Window\
            .partitionBy("date", "from_station_id", "part_time", "holiday", "week_days", "month")\
            .orderBy(desc("count"))

        mf_condition_table = group\
                        .withColumn("rank", row_number().over(window)).where("rank = 1")\
                        .withColumnRenamed("weather_description", "weather_description_mf") \
                        .withColumnRenamed("date", "date_mf") \
                        .withColumnRenamed("part_time", "part_time_mf") \
                        .withColumnRenamed("from_station_id", "from_station_id_mf") \
            .select("date_mf", "weather_description_mf", "part_time_mf", "from_station_id_mf")

        return mf_condition_table

    def group_target(self, dataframe, mf_condition_part_time):

        dataframe_joined = dataframe.join(mf_condition_part_time,
                    [dataframe.date == mf_condition_part_time.date_mf,
                    dataframe.from_station_id == mf_condition_part_time.from_station_id_mf,
                    dataframe.part_time == mf_condition_part_time.part_time_mf])

        dataframe_target = dataframe_joined.\
            groupBy("date", "from_station_id", "latitude", "longitude",
                    "mean_dpcapacity_start", "mean_dpcapacity_end",
                    "sum_subscriber", "sum_customer",
                    "part_time", "holiday", "week_days", "weather_description_mf", "month")\
            .agg(
                round(mean("humidity"), 1).alias("humidity"),
                round(mean("pressure"), 1).alias("pressure"),
                round(mean("temperature"), 1).alias("temperature"),
                round(mean("wind_speed"), 1).alias("wind_speed"),
                count("from_station_id").alias("bicycle_rentals"))

        return dataframe_target
