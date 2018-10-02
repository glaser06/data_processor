from collections import defaultdict
import os
from decimal import Decimal
import time
import threading as th
import pandas as pd
import ccxt
from ccxt import NetworkError, DDoSProtection, RequestTimeout, ExchangeNotAvailable, InvalidNonce
import requests
import numpy as np
import os
from influxdb import DataFrameClient
from influxdb import InfluxDBClient
from urllib.parse import urlparse
aiven_string = 'https+influxdb://avnadmin:b3zzovkrjk6y2dda@influx-3bc445f2-octobot-dev.aivencloud.com:10730/defaultdb'# os.environ.get('AIVEN_DEV_URL')
aiven_prod_string = 'https+influxdb://avnadmin:v3aqhn5rluinpj3n@influx-18787c32-octobot-production.aivencloud.com:29841/defaultdb'# os.environ.get('AIVEN_DEV_URL')
cli = InfluxDBClient.from_dsn(aiven_string, ssl=True, verify_ssl=False)
pd_cli = DataFrameClient.from_dsn(aiven_string, ssl=True, verify_ssl=False)
price_cli = DataFrameClient.from_dsn(aiven_prod_string, ssl=True, verify_ssl=False)
def average(numbers):
    return sum(numbers)/len(numbers)


def weighted_average(values, weights):
    if len(weights) == 0:
        raise Exception('No volumes received in weighted average')
    return sum([w * x for (w,x) in zip(weights, values)])/sum(weights)
# def standard_deviation(values):
#     n = len(values)
#     u = average(values)
#     diff_sum = 0
#     for val in values:
#         diff = val - u
#         diff_sum += diff * diff
#
#     sigma = math.sqrt(diff_sum/n)

def standard_deviation():
    pass
def load_prices():
    btc_price = price_cli.query('SELECT * FROM "price.BTC"  order by time desc limit 50000 offset 0')['price.BTC']
    btc_price['btc_price'] = btc_price['value']
    usdt_price = price_cli.query('SELECT * FROM "price.USDT"  order by time desc limit 50000 offset 0')['price.USDT']
    usdt_price['usdt_price'] = usdt_price['value']
    return btc_price, usdt_price
def stability(btc_price, usdt_price, process_rows=None):
    binance_btc_key= 'binance.TUSD/BTC.OHLCV5M'
    binance_usdt_key = 'binance.TUSD/USDT.OHLCV5M'
    bittrex_btc_key = 'bittrex.TUSD/BTC.OHLCV5M'

    # print(btc_price.index[0])
    exchanges = []
    tusdbtc_keys = [binance_btc_key, bittrex_btc_key]
    tusdusdt_keys = [binance_usdt_key]
    print('start loops')
    for key_sets in [tusdbtc_keys, tusdusdt_keys]:
        frames = []
        for key in key_sets:
            frame = pd_cli.query('select * from "{}" limit 5000 offset 0 '.format(key))
            frame = frame[key]
            print(frame)
            frame['price'] = frame['high']/3 + frame['low']/3 + frame['close']/3
            frames.append(frame)
        frames += [btc_price, usdt_price]
        res = pd.concat(frames, sort=True)
        print(res.head())
        return res


    markets = []
    key = binance_btc_key
    # a =
    prices = []
    # print(type(a[key]))
    # print(a[key])

    std = np.std(prices)
    print(std)


class Cruncher:
    def __init__(self, ):
        self.funcs = [stability]
    def get_crunchin(self, forever=True):
        while forever:
            self.execution()
    def execution(self):
        for each in self.funcs:
            each()
