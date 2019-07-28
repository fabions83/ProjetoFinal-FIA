# -*- coding: utf-8 -*-

from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator


class TrainingDataframe():

    def __init__(self, spark):
        self.spark = spark

    def preprocessing(self):

        model = GBTRegressor(labelCol="bicycle_rentals")

        cols = ["part_time", "holiday", "week_days", "weather_description_mf", "month"]

        imputer = Imputer(inputCols=["humidity", "pressure"],
                          outputCols=["humidity_input", "pressure_input"])

        indexers = [
            StringIndexer(inputCol=col, outputCol="{0}_indexed".format(col))
            for col in cols
        ]

        assembler = VectorAssembler(
            inputCols=["part_time_indexed", "holiday_indexed", "month_indexed",
                       "week_days_indexed", "weather_description_mf_indexed",
                       "humidity_input", "pressure_input", "temperature", "wind_speed",
                       "from_station_id", "mean_dpcapacity_start", "mean_dpcapacity_end",
                       "sum_subscriber", "sum_customer"],
            outputCol="features"
        )

        pipeline = Pipeline(stages=[imputer] + indexers + [assembler] + [model])

        return pipeline

    def training(self, pipeline, train):

        return pipeline.fit(train)

    def testing(self, pipeline, test):

        return pipeline.transform(test)

    def evaluator(self, predictions):

        regression_eval = RegressionEvaluator(labelCol="bicycle_rentals",
                                              predictionCol="prediction", metricName="rmse")
        rmse = regression_eval.evaluate(predictions)
        return rmse



