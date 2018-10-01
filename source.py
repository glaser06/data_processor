from collections import defaultdict
import os
from decimal import Decimal
import time
import threading as th
import ccxt
from ccxt import NetworkError, DDoSProtection, RequestTimeout, ExchangeNotAvailable, InvalidNonce
import requests
import os
from urllib.parse import urlparse
from influxdb import InfluxDBClient
from datetime import datetime, timedelta
import sys
import json
# import pandas as pd
import time

aiven_influxdb_url = 'https+influxdb://avnadmin:b3zzovkrjk6y2dda@influx-3bc445f2-octobot-dev.aivencloud.com:10730/defaultdb'
#
influxdb = InfluxDBClient.from_dsn(
    aiven_influxdb_url, ssl=True, verify_ssl=True)

def send_metric(metric, timestamp, value=None, fields=None):
    #print('sent {} = {} {}'.format(metric, value, datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ')))
    if 'funds' in metric:
        metric = metric + '.total'
    if value and float(value) <= 0.0:
        return
    if value:
        json_body = [{
            "measurement": metric,
            "time": (datetime.fromtimestamp(timestamp)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            "fields": {
                "value": float(value)
            }
        }]
    elif fields:
        json_body = [{
            "measurement": metric,
            "time": (datetime.fromtimestamp(timestamp)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            "fields": fields
        }]
    return
    influxdb.write_points(json_body)
def send_all_metrics(metrics):
    return
    influxdb.write_points(metrics)

def construct_metric(metric, timestamp, value=None, fields=None):
    if value:
        json_body = [{
            "measurement": metric,
            "time": (datetime.fromtimestamp(timestamp)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            "fields": {
                "value": float(value)
            }
        }]
    elif fields:
        json_body = [{
            "measurement": metric,
            "time": (datetime.fromtimestamp(timestamp)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            "fields": fields
        }]
    return json_body


class Source:
    def __init__(self, name, markets_to_run={}):
        # self.delegate = delegate
        self.name = name
        self.exchange = eval('ccxt.%s ()' % name)
        self.exchange.load_markets()
        if not markets_to_run:
            parameters = {
            'fetchTicker':[],# ['TUSD/USDT','TUSD/BTC','TUSD/ETH', 'USDT/TUSD', 'BTC/TUSD','ETH/TUSD'],
            'fetchOrderBook':  ['TUSD/USDT', 'BTC/USDT', 'TUSD/BTC', 'BTC/USD', 'USDT/TUSD','TUSD/ETH'],
            'fetchOHLCV': [], #['TUSD/USDT', 'BTC/USDT', 'TUSD/BTC'],
            'fetchTrades':[],# ['TUSD/USDT', 'BTC/USDT', 'TUSD/BTC'],
        }
        else:
            parameters = markets_to_run

        self.functions = [
            'fetchTicker',
            'fetchOrderBook',
            'fetchOHLCV',
            'fetchTrades',
        ]
        self.parameters = parameters
        self.triggers = [] # array of string ids
        self.timeout = 0.3

    def fetch_historical(self, symbol, from_datetime='2017-10-01 00:00:00'):
        msec = 1000
        minute = 60 * msec
        hold = 30
        exchange = self.exchange

        from_timestamp = exchange.parse8601(from_datetime)

        # -----------------------------------------------------------------------------

        now = exchange.milliseconds()

        # -----------------------------------------------------------------------------

        data = []

        while from_timestamp < now:

            try:

                print(exchange.milliseconds(), 'Fetching candles starting from', exchange.iso8601(from_timestamp))
                ohlcvs = exchange.fetch_ohlcv(symbol, '5m', from_timestamp)
                print(exchange.milliseconds(), 'Fetched', len(ohlcvs), 'candles')
                first = ohlcvs[0][0]
                last = ohlcvs[-1][0]
                print('First candle epoch', first, exchange.iso8601(first))
                print('Last candle epoch', last, exchange.iso8601(last))
                from_timestamp += len(ohlcvs) * minute * 5
                data += ohlcvs
                time.sleep(0.7)

            except (ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as error:

                print('Got an error', type(error).__name__, error.args, ', retrying in', hold, 'seconds...')
                time.sleep(hold)
        self.send_candles(data)


    def send_market_price(self,id, orderbook):
        name, market, func = id.split('.')
        metric = '.'.join([name, market, 'MarketPrice'])
        bid = orderbook['bids'][0][0] if len (orderbook['bids']) > 0 else None
        ask = orderbook['asks'][0][0] if len (orderbook['asks']) > 0 else None
        spread = (ask - bid) if (bid and ask) else None
        # print (id, 'market price', { 'bid': bid, 'ask': ask, 'spread': spread })
        send_metric(id, time.time(), fields={ 'bid': bid, 'ask': ask, 'spread': spread })

    def send_orderbook(self, id, orderbook):
        name, market, func = id.split('.')
        metric = '.'.join([name, market, 'OrderbookRaw'])
        metric = construct_metric(id, time.time(), fields={
            'bids': json.dumps(orderbook['bids']),
            'asks': json.dumps(orderbook['asks']),
        })
        # print(metric)
        send_all_metrics(metric)
    def send_liquidity(self, id, orderbook):
        def liquidity(orderbook, amount, side=False):
            # print(orderbook)
            # side= false for buys
            # side= true for sells
            in_quote = {a: (a * b) for a,b in orderbook.items()}
            starting = 0.0
            real_amount = 0.0
            # print(sum(orderbook.values()))
            # print('buy', sorted(in_quote.keys(), reverse=False))
            for each in sorted(orderbook.keys(), reverse=side):
                qty = orderbook[each]
                quote = qty * each
                # print(each, qty, quote)
                # print(starting, qty, starting + qty)
                if starting + qty <= amount :
                    # print(starting,amount)

                    starting += qty

                    real_amount += quote
                    # print(starting, real_amount)
                else:
                    leftover = amount - starting

                    real_amount += leftover * each
                    return real_amount
            # for each in sorted(in_quote.keys(), reverse=side):
            #     # print('buys', each)
            #     q = orderbook[each]
            #     if starting + q <= amount :
            #         # print(starting,amount)
            #         # print(starting, q, starting + q)
            #         starting += q
            #
            #         real_amount += in_quote[each]
            #         # print(starting, real_amount)
            #     else:
            #         leftover = amount - starting
            #
            #         real_amount += leftover * each
            # print(real_amount)
            return real_amount
            # raise Exception('not enough')

        str, quote = id.split('/')

        base = str.split('.')[-1]

        asset_amounts = {
            'BTC': 30,
            'ETH': 300,
            'USD': 100000,
            'TUSD': 100000,
            'USDT': 100000,
        }
        how_much = asset_amounts[base]

        print(how_much)
        # orderbook = data[self.exchange][self.api_call][self.market]
        bids, asks = defaultdict(float), defaultdict(float)
        bids_list = orderbook['bids']
        asks_list = orderbook['asks']
        print(id,asks_list[0])
        print(id,bids_list[0])
        for price, quantity in bids_list:

            # print(id,price, quantity)
            bids[price] += quantity

        for price, quantity in asks_list:
            asks[price] += quantity
        # print('bids',bids)
        buys = liquidity(asks, how_much, side=False)
        sells = liquidity(bids, how_much, side=True)
        print(buys-sells)
        print('ask', id,buys, buys/how_much)
        print('bid', id,sells, sells/how_much)
        # self.delegate.big_data['Output']['SingleLiquidityProcessor'][self.exchange][self.market]['ask'] = buys
        # self.delegate.big_data['Output']['SingleLiquidityProcessor'][self.exchange][self.market]['bid'] = sells
        # print('processing',self.delegate.big_data['Output']['ask'])
        return buys, sells

    def send_candles(self,id, candles):
        name, market, func = id.split('.')
        metric = '.'.join([name, market, 'OHLCV5M'])
        metrics = []
        for candle in candles:

            metrics += construct_metric(id, candle[0], fields={
                'timestamp': candle[0],
                'open': candle[1],
                'high': candle[2],
                'low': candle[3],
                'close': candle[4],
                'vol': candle[5],
            })
        send_all_metrics(metrics)
    def update(self):

        def execute(func, param):
            # print('what')
            id = '.'.join([self.name, param, func])
            output = getattr(self.exchange, func)(param)
            print('\n ',self.name, func, '\n ')
            # print(output)
            if func == 'fetchOrderBook':
                self.send_market_price(id,output)
                self.send_orderbook(id, output)
                self.send_liquidity(id, output)
            elif func == 'fetchOHLCV':
                self.send_candles(id,output)
            print('worked?', self.name, func, param)
            # send_metric(id,  time.time(), fields=output)
            # print('Output',self.delegate.big_data['Output'])



        data = defaultdict(dict)
        functions = self.functions
        threads = []
        for func in functions:
            if self.exchange.has[func]:
                # print(self.name,'has',func)
                # continue
                for param in self.parameters[func]:
                    if param in self.exchange.markets:

                        t = th.Thread(target=execute, args=(func, param))
                        t.start()
                        threads.append(t)
                        # time.sleep(self.timeout)
        [t.join() for t in threads]
        return data
print("\n\n\n\n ### STARTING RUN ### \n\n\n--------------------------\n\n")
exchanges = [
    # 'binance',
    # 'bittrex',
    # 'hitbtc',
    # 'coinbase',
    'coinbasepro',
    'coinmarketcap',
    'bitstamp',
]
parameters = {
'fetchTicker':[],# ['TUSD/USDT','TUSD/BTC','TUSD/ETH', 'USDT/TUSD', 'BTC/TUSD','ETH/TUSD'],
'fetchOrderBook':  ['TUSD/USDT', 'BTC/USDT', 'TUSD/BTC', 'BTC/USD', 'USDT/TUSD','TUSD/ETH'],
'fetchOHLCV': [], #['TUSD/USDT', 'BTC/USDT', 'TUSD/BTC'],
'fetchTrades':[],# ['TUSD/USDT', 'BTC/USDT', 'TUSD/BTC'],
}
s = [Source(name, markets_to_run=parameters) for name in exchanges]
# [a.update() for a in s]
[a.fetch_historical('BTC/USD') for a in s]
