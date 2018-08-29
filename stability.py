import time 
import os 

import ccxt


def ohlcvs_from_exchange(exchange, pair, basis, from_time='2018-05-29 00:00:00'):
    
    msec = 1000
    minute = 60 * msec
    hold = 30

    # -----------------------------------------------------------------------------


    exchange = ccxt.bittrex({
        'enableRateLimit': True,
        # 'verbose': True,
    })

    # -----------------------------------------------------------------------------

    from_datetime = '2018-05-21 00:00:00'
    from_timestamp = exchange.parse8601(from_datetime)

    # -----------------------------------------------------------------------------

    now = exchange.milliseconds()

    # -----------------------------------------------------------------------------

    data = []
    stability = 0.0
    count = 0
    while from_timestamp < now:

        try:

            print(exchange.milliseconds(), 'Fetching candles starting from', exchange.iso8601(from_timestamp))
            ohlcvs = exchange.fetch_ohlcv('TUSD/BTC', '5m', from_timestamp)
            usdt_ohlcvs = exchange.fetch_ohlcv('BTC/USD', '5m', from_timestamp)

            for ohlcv, usdt_ohlcv in zip(ohlcvs, usdt_ohlcvs):
                new_ohlcv = [a * b for (a,b) in zip(ohlcv, usdt_ohlcv)]
                new_ohlcv = new_ohlcv[1:]

                del new_ohlcv[-1]
                condition = [a > 2 or a < 0.95 for a in new_ohlcv]
                if any([a > 2 or a < 0.95 for a in new_ohlcv]): 
                    print(ohlcv, usdt_ohlcv)
                    print('wtf, ' , new_ohlcv)
                high = abs(1 - max(new_ohlcv)) * 100
                low = abs(1 - min(new_ohlcv)) * 100
                stability_new = (high + 2 * low) ** 2
                if stability_new > 1000: 
                    print(high, low)
                    continue
                stability += stability_new
                print(new_ohlcv)
                count += 2
            print('Stability, ', stability/float(count))
            print(exchange.milliseconds(), 'Fetched', len(ohlcvs), 'candles')
            first = ohlcvs[0][0]
            last = ohlcvs[-1][0]
            print('First candle epoch', first, exchange.iso8601(first))
            print('Last candle epoch', last, exchange.iso8601(last))
            from_timestamp += len(ohlcvs) * minute * 5
            data += ohlcvs

        except (ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as error:

            print('Got an error', type(error).__name__, error.args, ', retrying in', hold, 'seconds...')
            time.sleep(hold)
    print(stability/float(count))
    return data

ohlcvs_from_exchange('binance', 'TUSD/BTC', 'BTC/USDT')
