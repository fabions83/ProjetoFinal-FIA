#!/usr/bin/python3
# -*- coding: utf-8 -*-

from flask import Flask, jsonify
from kafka import KafkaConsumer


app = Flask(__name__)


@app.route('/bikes')
def predict():

    consumer = KafkaConsumer('bikes',
                             auto_commit_interval_ms=1000,
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             max_poll_records=5000,
                             max_partition_fetch_bytes=327344026,
                             bootstrap_servers=['quickstart.cloudera:9092'],
                             group_id='bikes_group')

    msg_pack = consumer.poll(timeout_ms=6000)

    result = []
    for tp, messages in msg_pack.items():
        for message in messages:
            result.append(message.value.decode())

    consumer.close()

    return jsonify(result)


if __name__ == '__main__':

    app.run(host='0.0.0.0', debug=True)

