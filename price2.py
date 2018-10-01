from collections import defaultdict
import os
from decimal import Decimal
import time
import threading as th
import pypd
import ccxt
from ccxt import NetworkError, DDoSProtection, RequestTimeout, ExchangeNotAvailable, InvalidNonce
import requests
from .price_values import Values
import pika, os
from urllib.parse import urlparse

def vividict():
    return defaultdict(vividict)

class Price:

    def __init__(self):
        self.values = Values()

        self.emas = vividict()
        self.volumes = vividict()
        self.pipelines = vividict()
        self.usd_prices = defaultdict(lambda: Decimal(1.0))
        self.ema_generators = vividict()
        self.keep_alive = defaultdict(bool)
        self.last_update_time = defaultdict(float)

        self.tickers = vividict()
        self.error_counter = defaultdict(int)

        url_str = os.environ.get('CLOUDAMQP_URL',
                                 'amqp://guest:guest@localhost//')
        url = urlparse(url_str)
        self.pika_params = pika.ConnectionParameters(
            host=url.hostname,
            virtual_host=url.path[1:],
            credentials=pika.PlainCredentials(url.username, url.password))
        self.make_connection()
        self.exchanges = []
        self.exchanges_started = set()
        self.usd_prices['BTC/USD'] = Decimal(1.0)
        self.usd_prices['ETH/USD'] = Decimal(1.0)

    def gogogo(self):
        self.main_loop(forever=False)
        t = th.Thread(target=self.main_loop, daemon=True)
        t.start()
        self.start_calc()
        t.join()

    def parse_and_update(self, symbol_ticker, exchange):
        pair = symbol_ticker['symbol']
        self.tickers[pair][exchange.id] = symbol_ticker

    def setup_exchanges(self, exchanges_to_pull):

        to_deactivate = self.exchanges_started - exchanges_to_pull
        for feed in self.exchanges_started:
            if feed in to_deactivate:
                self.keep_alive[feed] = False
                print('Deactivating {}'.format(feed))
            else:
                if not self.keep_alive[feed]:
                    print('Activating {}'.format(feed))
                self.keep_alive[feed] = True

        to_launch = exchanges_to_pull - self.exchanges_started
        exchanges = []
        for feed in to_launch:
            exc = feed[0]
            pair = feed[1]
            exchange = eval('ccxt.%s ()' % exc)
            self.keep_alive[feed] = True
            exchanges.append(exchange)
            self.exchanges.append(exchange)
            exchange.load_markets()
            self.tickers[pair][exc] = {}
            # TODO: put this in postgres
            self.ema_generators[pair][exc] = self.ema(Decimal(0.5))
            next(self.ema_generators[pair][exc])

        return exchanges

    def main_loop(self, forever=True):

        while True:
            try:
                # grab new exchanges and pairs
                self.values.update_values()
                exchanges = self.setup_exchanges(self.values.exchange_feeds)
                if len(exchanges) > 0:
                    self.start(exchanges)
                if not forever:
                    break
                time.sleep(5.0)
            except Exception as e:
                # handles postgres errors
                if self.handle_exception(e, 'ERROR: updating exchanges'):
                    return
                if not forever:
                    break

    def sleep_if_needed(self, exchange_id):
        prev_time = Decimal(self.last_update_time[exchange_id])
        rate_limit = Decimal(self.values.rate_limits[exchange_id])
        curr_time = Decimal(time.time())

        if curr_time - prev_time < rate_limit:
            time.sleep(rate_limit - Decimal(curr_time - prev_time))


    def handle_exception(self, e, id):
        self.error_counter[id] += 1
        print('{} ERROR {}: {}'.format(id, self.error_counter[id], e))
        time.sleep(self.values.retry_wait)
        if self.error_counter[id] >= self.values.errors_max:
            self.send_pagerduty(str(e), 'error')
            return True
        return False


    def fetch_with_single(self, pair, exchange):
        while True:
            feed = (exchange.id, pair)
            if not self.keep_alive[feed]:
                continue
            try:
                self.sleep_if_needed(exchange.id)
                self.last_update_time[exchange.id] = time.time()
                data = exchange.fetch_ticker(pair)
                self.parse_and_update(data, exchange)
                self.error_counter[exchange.id] = 0
            except DDoSProtection as e :
                # sleep for 4 minutes to wait out typical
                # ip ban time
                print("DDoS Protection for", exchange.id)
                self.handle_exception(e, exchange.id)
                self.tickers[pair][exchange.id]['quoteVolume'] = Decimal(0)
                self.tickers[pair][exchange.id]['baseVolume'] = Decimal(0)
                time.sleep(240)
            except InvalidNonce as e:
                # possible rate limit issue
                print("Invalid nonce for", exchange.id)
                self.handle_exception(e, exchange.id)
                self.tickers[pair][exchange.id]['quoteVolume'] = Decimal(0)
                self.tickers[pair][exchange.id]['baseVolume'] = Decimal(0)
                time.sleep(30)
            except ExchangeNotAvailable as e :
                # when exchanges down, immediately send pagerduty,
                # whoever responds needs to turn it off in Postgres
                print("{} is currently down".format(exchange.id))
                self.error_counter[exchange.id] += self.values.errors_max
                self.tickers[pair][exchange.id]['quoteVolume'] = Decimal(0)
                self.tickers[pair][exchange.id]['baseVolume'] = Decimal(0)
                self.handle_exception(e, exchange.id)
            except Exception as e:
                if self.handle_exception(e, exchange.id):
                    return

    def start(self, exchanges):
        symbols = list(self.tickers.keys())

        for exchange in exchanges:
            for pair, feed_exchanges in self.tickers.items():
                feed = (exchange.id, pair)
                if exchange.id in feed_exchanges and feed not in self.exchanges_started:
                    t = th.Thread(target=self.fetch_with_single, args=(pair, exchange), daemon=True)
                    feed = (exchange.id, pair)
                    self.exchanges_started.add(feed)
                    self.keep_alive[feed] = True
                    print('started single', feed)
                    t.start()
                    time.sleep(2.123)
                elif exchange.id in feed_exchanges and feed in self.exchanges_started:
                    self.keep_alive[feed] = True
                    print('restarted {} on {}'.format(pair, exchange.id))

    def make_connection(self):
        connection = pika.BlockingConnection(
            self.pika_params)  # Connect to CloudAMQP
        channel = connection.channel()  # start a channel
        channel.exchange_declare(
            exchange='price', exchange_type='fanout')
        bitt_queue = channel.queue_declare(queue='bittrex_bot')
        channel.queue_bind(
            exchange='price', queue=bitt_queue.method.queue)
        return channel

    #TODO expand pagerduty alerts, move to separate file.
    def send_pagerduty(self, message, severity, merge=None):
        if self.values.dev_pager == 1:
            print('Would have sent ({}, {}) to pagerduty'.format(
                message, severity))
            return
        if merge and self.incident_ids.get(merge):
            ret = pypd.EventV2.create(
                data={
                    'routing_key': self.values.pagerduty_routing_key,
                    'dedup_key': self.incident_ids[merge],
                    'event_action': 'trigger',
                    'payload': {
                        'summary': message,
                        'severity': severity,
                        'source': 'Market Making Bot',
                    }
                })
        else:
            ret = pypd.EventV2.create(
                data={
                    'routing_key': self.values.pagerduty_routing_key,
                    'event_action': 'trigger',
                    'payload': {
                        'summary': message,
                        'severity': severity,
                        'source': 'Market Making Bot',
                    }
                })
            if merge:
                self.incident_ids[merge] = ret['dedup_key']
        return

    @staticmethod
    def average(numbers):
        return sum(numbers)/len(numbers)

    @staticmethod
    def weighted_average(values, weights):
        if len(weights) == 0:
            raise Exception('No volumes received in weighted average')
        return sum([w * x for (w,x) in zip(weights, values)])/sum(weights)

    @staticmethod
    def ema(alpha):
        temp = yield
        current, average = temp, temp
        while True:
            average = current * alpha + average * (1 - alpha)
            current = yield average

    def calculate_price_for_pair(self, pair):
        exchanges = self.tickers[pair]
        prices = []
        volumes = []
        for exchange, ticker in exchanges.items():
            feed = (exchange, pair)

            if not self.keep_alive[feed]:
                continue
            if 'last' in ticker:
                avg = self.average([
                    Decimal(ticker['last']),
                    Decimal(ticker['bid']),
                    Decimal(ticker['ask'])
                ])
                ema = self.ema_generators[pair][exchange].send(avg)
                prices.append(ema)

                # POTENTIAL BUG: volume format incorrect for ccxt
                base_vol = Decimal(ticker['baseVolume'])
                if ticker['quoteVolume'] is None:
                    quote_vol = base_vol * ema
                else:
                    quote_vol = Decimal(ticker['quoteVolume'])
                volumes.append(quote_vol)
            # TODO: raise error for no ticker data

        price = self.weighted_average(prices, volumes)
        return price, sum(volumes)

    def loop_main_calc(self):
        channel = self.make_connection()
        while True:
            try:
                self.calculate_all_main(channel)
                # waits haif a second for data to update
                # and so that we don't overload rabbitmq
                time.sleep(0.5)
            except Exception as e:
                print('main error on btc or eth', e)

                if self.handle_exception(e, 'main_pairs'):
                    return

    def loop_alt_calc(self):
        channel = self.make_connection()
        while True:
            try:
                self.calculate_all_altcoins(channel)
                # waits haif a second for data to update
                # and so that we don't overload rabbitmq
                time.sleep(0.5)
            except Exception as e:
                if self.handle_exception(e, 'alt_coins'):
                    return

    def start_calc(self):
        main_price_thread = th.Thread(
            target=self.loop_main_calc,
            daemon=True)
        altcoin_price_thread = th.Thread(
            target=self.loop_alt_calc,
            daemon=True)
        main_price_thread.start()
        altcoin_price_thread.start()
        main_price_thread.join()
        altcoin_price_thread.join()

    def calculate_all_altcoins(self, channel):
        pairs = self.tickers.keys()
        for symbol in self.values.coins:
            if symbol in ['BTC', 'ETH']:
                continue
            pairs_that_contain = [(pair, self.calculate_price_for_pair(pair) ) for pair in pairs if symbol in pair]
            symbol_usd_prices = []
            symbol_usd_volumes = []
            for pair, (price, volume) in pairs_that_contain:
                try :
                    base, quote = pair.split('/')
                    if quote == "USD":
                        continue
                    basis_symbol = quote if base == symbol else base
                    usd_basis = basis_symbol+'/USD'
                    base_price = Decimal(self.usd_prices[usd_basis]) * Decimal(price)
                    quote_price = Decimal(self.usd_prices[usd_basis]) / Decimal(price)
                    usd_price = base_price if base == symbol else quote_price
                    symbol_usd_prices.append(usd_price)
                    usd_volume = volume * usd_price
                    symbol_usd_volumes.append(usd_volume)

                except Exception as e:
                    if self.handle_exception(e, pair):
                        return
            real_usd_price = self.weighted_average(symbol_usd_prices, symbol_usd_volumes)
            self.send_price(symbol, real_usd_price, channel)

    def calculate_all_main(self, channel):
        # main pairs are BTC and ETH
        # we calculate altcoin prices from main pairs,
        #   so they are updated on separate threads
        try:
            price, volume = self.calculate_price_for_pair('BTC/USD')
            self.usd_prices['BTC/USD'] = price
            self.send_price('BTC', self.usd_prices['BTC/USD'], channel)
        except Exception as e:
            if self.handle_exception(e, 'main-loop-BTC/USD'):
                return

        try:
            price, volume = self.calculate_price_for_pair('ETH/USD')
            self.usd_prices['ETH/USD'] = price
            self.send_price('ETH', self.usd_prices['ETH/USD'], channel)
        except Exception as e:
            if self.handle_exception(e, 'main-loop-ETH/USD'):
                return

    def send_price(self, currency, final_price, channel):
        self.usd_prices[currency] = final_price
        # if currency not in ['BTC', 'ETH']:
        print('{} price: {}'.format(currency,
                        final_price))
        channel.basic_publish(
            exchange='price',
            routing_key='',
            body='{},{}'.format(currency, final_price))


if __name__ == '__main__':
    price_fetcher = Price()
    price_fetcher.gogogo()
