from collections import defaultdict
import os
from decimal import Decimal
import time

import threading as th

import ccxt
from ccxt import NetworkError, DDoSProtection, RequestTimeout, ExchangeNotAvailable, InvalidNonce
import requests

import  os
from urllib.parse import urlparse

from source import Source

from processor import *

class ControllerInput: 
    def __init__(self): 
        pass
    def update(self): 
        pass
    

def vividict():
    return defaultdict(vividict)
class DataController:

    # source id protocol
    # api.exchange.market

    def __init__(self):
        self.parameters = ControllerInput()
        self.threads = []
        self.exchanges = []
        self.sources = {}
        self.triggers = {
            'fetchOrderBook.binance.BTC/USDT': ['SingleLiquidityProcessor'],
            'fetchOrderBook.binance.TUSD/BTC': ['SingleLiquidityProcessor'],
            'fetchOrderBook.binance.TUSD/USDT': ['SingleLiquidityProcessor'],
        } # 'source ids' : [processors]
        
        self.queue = []
        self.big_data = defaultdict(dict)

        self.big_data['SingleLiquidityProcessor'] = {
            'BTC/USDT': 20.0,
            'TUSD/USDT': 100000.0,
            'TUSD/BTC': 50000.0,
        }
        self.big_data['Output'] = vividict()
    
    def update_params(self, market, amount): 
        self.big_data['SingleLiquidityProcessor'][market] = amount

    def producer(self, exchange): 
        while True: 
            # print('producer',self.big_data['Output'])
            data = self.sources[exchange].update()
            # self.big_data[exchange] = data
            # for each in self.sources[exchange]:
            #     [self.queue.append(id) for id in each.triggers]
    
    def exit(self): 
        [t.join() for t in self.threads]
        # self.threads.join()
        self.queue[0] = 'EXIT'

    def threaded_start(self):
        thread = th.Thread(target=self.start, daemon=True)
        thread.start()
        self.threads.append(thread)
        
    
    def start(self): 
        worker_thread = th.Thread(name='worker_thread', target=self.worker_loop)
        worker_thread.start()
        threads = [worker_thread]
        for each in self.exchanges: 
            functions = [
                'fetchTicker',
                'fetchOrderBook',
                'fetchOHLCV',
                'fetchTrades',
            ]
            for func in functions: 
                self.big_data[each][func] = {}
            self.sources[each] = Source(each, delegate=self)
            print(each)
            thread = th.Thread(name=id, target=self.producer, args=(each,))
            threads.append(thread)
            thread.start()
        [t.join(timeout=1) for t in threads]

    def worker(self, processor): 
        processor.process(self.big_data)

    def worker_loop(self): 

        while True: 
            
            for id in self.queue: 
                if id == 'EXIT':
                    [t.join() for t in threads]
                    return
                api, exchange, market = id.split('.')
                if self.triggers[id]:
                    Processor = globals()[self.triggers[id][0]]
                    if self.triggers[id][0] == 'SingleLiquidityProcessor':
                        amount = 50000.0
                        if market == 'TUSD/USDT':
                            amount = 100000.0
                        elif market == 'BTC/USDT':
                            amount = 20.0

                        processor = Processor(api=api, exchange=exchange, market=market, amount=amount, delegate=self)
                    else:
                        processor = Processor(api=api, exchange=exchange, market=market)
                    thread = th.Thread(name=id, target=self.worker, args=(processor,), daemon=True)
                    self.threads.append(thread)
                    thread.start()
                    self.queue.pop()
            [t.join() for t in self.threads]


# a = DataController()
# a.exchanges = ['binance']
# a.start()

        