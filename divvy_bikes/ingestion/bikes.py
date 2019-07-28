# -*- coding: utf-8 -*-

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, row_number, count, round, mean, when, col, sum


class BikesDataframe():

    def __init__(self, spark):
        self.spark = spark

    def create(self):

        dataframe = self.spark.read.format("csv")\
                        .option("header", "true")\
                        .load("hdfs:///user/labdata/csv/data_raw.csv")\
        .withColumn("subscriber", when(col("usertype").isin("Subscriber"), 1).otherwise(0)) \
        .withColumn("customer", when(col("usertype").isin("Customer"), 1).otherwise(0))

        group = dataframe.groupBy("from_station_id", "latitude_start", "longitude_start")\
            .agg(count("from_station_id").alias("count"),
                 round(mean("dpcapacity_start")).alias("mean_dpcapacity_start"),
                 round(mean("dpcapacity_end")).alias("mean_dpcapacity_end"),
                 round(sum("subscriber")).alias("sum_subscriber"),
                 round(sum("customer")).alias("sum_customer"))

        window = Window.partitionBy("from_station_id").orderBy(desc("count"))

        dataframe_dedup_latlong = group.withColumn("rank", row_number().over(window))\
                                        .where("rank = 1")\
                                        .withColumnRenamed("from_station_id", "station")\
                                        .withColumnRenamed("latitude_start", "latitude")\
                                        .withColumnRenamed("longitude_start", "longitude")

        bikes = dataframe.join(dataframe_dedup_latlong,
                        dataframe.from_station_id == dataframe_dedup_latlong.station)

        return bikes
