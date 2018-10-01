from collections import defaultdict
import os
from decimal import Decimal
import time
import threading as th
from influxdb import InfluxDBClient
from datetime import datetime, timedelta
import ccxt
from ccxt import NetworkError, DDoSProtection, RequestTimeout, ExchangeNotAvailable, InvalidNonce
import requests

import os
from urllib.parse import urlparse


def average(numbers):
    return sum(numbers)/len(numbers)


def weighted_average(values, weights):
    if len(weights) == 0:
        raise Exception('No volumes received in weighted average')
    return sum([w * x for (w,x) in zip(weights, values)])/sum(weights)

# amount in base
# result in quote
def buy_liquidity(orderbook, amount):
    in_quote = {a: (a * b) for a,b in orderbook.items()}
    starting = 0.0
    real_amount = 0.0
    # print(sum(orderbook.values()))
    # print('buy', sorted(in_quote.keys(), reverse=False))
    for each in sorted(in_quote.keys(), reverse=False):
        # print('buys', each)
        q = orderbook[each]
        if starting + q <= amount :
            # print(starting,amount)
            starting += q
            real_amount += in_quote[each]
            # print(starting, real_amount)
        else:
            leftover = amount - starting

            real_amount += leftover * each
            # print('out',starting, real_amount)
            return real_amount
    raise Exception('not enough')
def sell_liquidity(orderbook, amount):
    in_quote = {a: (a * b) for a,b in orderbook.items()}
    starting = 0.0
    real_amount = 0.0
    # print(sum(orderbook.values()))
    # print('sell', sorted(in_quote.keys(), reverse=True))
    for each in sorted(in_quote.keys(), reverse=True):
        # print('sells', each)
        q = orderbook[each]
        if starting + q <= amount :
            # print(starting,amount)
            starting += q
            real_amount += in_quote[each]
            # print(starting, real_amount)
        else:
            leftover = amount - starting

            real_amount += leftover * each
            # print('out',starting, real_amount)
            return real_amount
    raise Exception('not enough')

# def slippage(expected, actual):




class Processor:
    def __init__(self, exchange='', market='', api='', coin=''):
        self.exchange = exchange
        self.market = market
        self.api_call = api

    def process(self, data):
        raise Exception('not implemented')
class TickerProcessor(Processor):
    def process(self, data):
        print(self.exchange, self.api_call, self.market)
        print(data[self.exchange][self.api_call])
        print(data[self.exchange])
        mine = data[self.exchange]
        # print(mine[self.api_call][self.market])

class PrintProcessor(Processor):
    def process(self, data):
        print(self.exchange, self.api_call, self.market)
        print(data[self.exchange][self.api_call])

class SingleLiquidityProcessor(Processor):
    def __init__(self, exchange='', market='', api='', coin='', amount=100000.0, delegate=None):
        super().__init__(exchange=exchange, market=market, api=api, coin=coin)
        self.amount = amount
        self.delegate = delegate

    def process(self, data):
        how_much = data['SingleLiquidityProcessor'][self.market]
        print(data['SingleLiquidityProcessor'][self.market])
        orderbook = data[self.exchange][self.api_call][self.market]
        bids, asks = defaultdict(float), defaultdict(float)
        bids_list = orderbook['bids']
        asks_list = orderbook['asks']
        for price, quantity in bids_list:
            bids[price] += quantity

        for price, quantity in asks_list:
            asks[price] += quantity
        # print('bids',bids)
        buys = buy_liquidity(asks, how_much)
        sells = sell_liquidity(bids, how_much)
        print(buys-sells)
        print('ask', self.market,buys, buys/how_much)
        print('bid', self.market,sells, sells/how_much)
        self.delegate.big_data['Output']['SingleLiquidityProcessor'][self.exchange][self.market]['ask'] = buys
        self.delegate.big_data['Output']['SingleLiquidityProcessor'][self.exchange][self.market]['bid'] = sells
        # print('processing',self.delegate.big_data['Output']['ask'])
        return buys, sells


class CandlestickProcessor(Processor):
    def process(self, data):
        pass

    def parse(self, data):
        pass
    def calculate(self, data):
        pass
    def output(self, data):
        pass
