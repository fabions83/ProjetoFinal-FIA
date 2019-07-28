# -*- coding: utf-8 -*-

import requests

url = "http://api.openweathermap.org/data/2.5/forecast?id=4887398&appid=84687a05b2471bbbb7d10400902ce144"


def get_weather():

    weather_json = requests.get(url)
    return weather_json.json()
