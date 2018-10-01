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

class Source:
    def __init__(self, name, delegate=None, parameters={}): 
        self.delegate = delegate
        self.name = name
        self.exchange = eval('ccxt.%s ()' % name)
        if not parameters:
            parameters = {
            'fetchTicker': [],
            'fetchOrderBook': ['TUSD/USDT', 'BTC/USDT', 'TUSD/BTC'],
            'fetchOHLCV': [],
            'fetchTrades': [],
        }
        self.parameters = parameters
        self.triggers = [] # array of string ids
        self.timeout = 1.0

    def update(self): 

        def execute(func, param):
            print('what')
            id = '.'.join([func, self.name, param])
            data[func][param] = getattr(self.exchange, func)(param)
            # print(data[func][param])
            # print(func,self.delegate.big_data[self.name][func])
            print('Output',self.delegate.big_data['Output'])
            self.delegate.big_data[self.name][func][param] = data[func][param]
            # print(func,self.delegate.big_data[self.name][func])
            self.delegate.queue.append(id)
            print('what2')
        functions = [
            'fetchTicker',
            'fetchOrderBook',
            'fetchOHLCV',
            'fetchTrades',
        ]
        data = defaultdict(dict)
        threads = []
        for func in functions: 
            for param in self.parameters[func]: 
                
                t = th.Thread(target=execute, args=(func, param))
                t.start()
                threads.append(t)
                time.sleep(self.timeout)
        [t.join() for t in threads]
        return data
            
    
    