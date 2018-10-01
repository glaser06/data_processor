from collections import defaultdict
import os
from decimal import Decimal
import time
import threading as th
import pandas as pd
import ccxt
from ccxt import NetworkError, DDoSProtection, RequestTimeout, ExchangeNotAvailable, InvalidNonce
import requests

import os
from influxdb import DataFrameClient
from influxdb import InfluxDBClient
from urllib.parse import urlparse
aiven_string = 'https+influxdb://avnadmin:b3zzovkrjk6y2dda@influx-3bc445f2-octobot-dev.aivencloud.com:10730/defaultdb'# os.environ.get('AIVEN_DEV_URL')
cli = InfluxDBClient.from_dsn(aiven_string, ssl=True, verify_ssl=False)
pd_cli = DataFrameClient.from_dsn(aiven_string, ssl=True, verify_ssl=False)

def average(numbers):
    return sum(numbers)/len(numbers)


def weighted_average(values, weights):
    if len(weights) == 0:
        raise Exception('No volumes received in weighted average')
    return sum([w * x for (w,x) in zip(weights, values)])/sum(weights)

class Cruncher:
    def __init__(self, ):
