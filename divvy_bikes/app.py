#!/usr/bin/python3
# -*- coding: utf-8 -*-

from utils.spark_utils import session_spark
from flask import Flask, request, jsonify


app = Flask(__name__)


@app.route('/bikes')
def predict():

    spark = session_spark()
    station = request.args.get('station', None)

    if station:
        js = spark.read.json("hdfs:///user/labdata/predict")\
                    .where("from_station_id = %s"%str(station))
    else:
        js = spark.read.json("hdfs:///user/labdata/predict")

    return jsonify(js.toJSON().collect())


if __name__ == '__main__':

    app.run(host='0.0.0.0', debug=True)

