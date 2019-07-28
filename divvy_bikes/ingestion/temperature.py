# -*- coding: utf-8 -*-
from pyspark.sql.functions import when, col, lower

thunderstorm = ["thunderstorm with light rain", "thunderstorm with rain",
                "thunderstorm with heavy rain", "light thunderstorm", "thunderstorm",
                "heavy thunderstorm" ,"ragged thunderstorm", "thunderstorm with light drizzle",
                "thunderstorm with drizzle", "thunderstorm with heavy drizzle",
                "proximity thunderstorm with rain", "proximity thunderstorm with drizzle",
                "proximity thunderstorm"]

drizzle = ["light intensity drizzle", "drizzle", "heavy intensity drizzle",
           "light intensity drizzle rain", "drizzle rain", "heavy intensity drizzle rain",
           "shower rain and drizzle", "heavy shower rain and drizzle", "shower drizzle"]

rain = ["light rain", "moderate rain", "heavy intensity rain", "very heavy rain", "extreme rain",
        "freezing rain", "light intensity shower rain", "shower rain", "heavy intensity shower rain",
        "ragged shower rain", "proximity shower rain"]

snow = ["light snow", "snow", "heavy snow", "sleet", "light shower sleet", "shower sleet",
        "light rain and snow", "rain and snow", "light shower snow", "shower snow",
        "heavy shower snow"]

atmosphere = ["mist", "smoke", "haze", "sand/ dust whirls", "fog", "sand", "dust",
              "Ash volcanic ash", "squalls", "tornado"]

clear = ["clear sky"]

clouds = ["few clouds", "scattered clouds", "broken clouds", "overcast clouds"]


class TemperatureDataframe():

    def __init__(self, spark):
        self.spark = spark

    def read(self, path):

        dataframe = self.spark.read.format("csv") \
            .option("header", "true") \
            .load(path) \
            .select("datetime", "Chicago")

        return dataframe

    def create_new_columns(self, dataframe):

        thunderstorm_broadcast = self.spark.sparkContext.broadcast(thunderstorm)
        drizzle_broadcast = self.spark.sparkContext.broadcast(drizzle)
        rain_broadcast = self.spark.sparkContext.broadcast(rain)
        snow_broadcast = self.spark.sparkContext.broadcast(snow)
        atmosphere_broadcast = self.spark.sparkContext.broadcast(atmosphere)
        clear_broadcast = self.spark.sparkContext.broadcast(clear)
        clouds_broadcast = self.spark.sparkContext.broadcast(clouds)

        dataframe = dataframe.withColumn("weather_condition", when(col("weather_description").isin(thunderstorm_broadcast.value), "Thunderstorm")\
                    .otherwise(when(lower(col("weather_description")).isin(drizzle_broadcast.value), "Drizzle")\
                    .otherwise(when(lower(col("weather_description")).isin(rain_broadcast.value), "Rain")\
                    .otherwise(when(lower(col("weather_description")).isin(snow_broadcast.value), "Snow")\
                    .otherwise(when(lower(col("weather_description")).isin(atmosphere_broadcast.value), "Atmosphere")\
                    .otherwise(when(lower(col("weather_description")).isin(clear_broadcast.value), "Clear")\
                    .otherwise(when(lower(col("weather_description")).isin(clouds_broadcast.value), "Clouds")\
                               .otherwise("Clouds"))))))))

        return dataframe

    def create(self):

        humidity = self.read("hdfs:///user/labdata/csv/humidity.csv")\
                        .withColumnRenamed("Chicago", "humidity")
        pressure = self.read("hdfs:///user/labdata/csv/pressure.csv") \
                        .withColumnRenamed("Chicago", "pressure")
        temperature = self.read("hdfs:///user/labdata/csv/temperature.csv") \
                            .withColumnRenamed("Chicago", "temperature")
        wind_speed = self.read("hdfs:///user/labdata/csv/wind_speed.csv")\
                            .withColumnRenamed("Chicago", "wind_speed")
        weather_description_read = self.read("hdfs:///user/labdata/csv/weather_description.csv")\
                                        .withColumn("Chicago",
                                            when(col("Chicago") == "sky is clear", "clear sky")
                                            .otherwise(col("Chicago")))\
                                        .withColumnRenamed("Chicago", "weather_description")
        weather_description = self.create_new_columns(weather_description_read)

        dataframe = humidity\
            .join(pressure, humidity.datetime == pressure.datetime) \
            .join(temperature, humidity.datetime == temperature.datetime) \
            .join(weather_description, humidity.datetime == weather_description.datetime) \
            .join(wind_speed, humidity.datetime == wind_speed.datetime) \
            .select(humidity.datetime,
                    humidity.humidity,
                    pressure.pressure,
                    temperature.temperature,
                    weather_description.weather_condition.alias("weather_description"),
                    wind_speed.wind_speed)

        return dataframe
