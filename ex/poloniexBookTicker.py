# -*- coding: utf8 -*-
from __future__ import print_function
from __future__ import division
from urllib import request
from pymongo import MongoClient
from bson import json_util
from datetime import datetime
import pytz, time
from colorama import init
from colorama import Fore, Back, Style
init()

interval = 30
exchange = 'Poloniex'
bookTickerURL = """https://poloniex.com/public?command=returnOrderBook&currencyPair=all&depth=1"""
data = {"_id":"0123456789", "timestamp":"1234567890","symbol": "LTCBTC", "bidPrice": "0.9", "bidQty": "123", "askPrice": "0.9", "askQty": "456"}

client = MongoClient()
db = client.poloniex_bookTicker

d = datetime.now(tz=pytz.utc)
dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
print("{} {} bookTicker started...".format(dFormatted, exchange))

while True:
    d = datetime.now(tz=pytz.utc)
    ts = time.time()
    if(not d.second % interval):
        print ("{} Interval".format(d.strftime('%d/%m/%Y %H:%M:%S')))
        try:
            response = request.urlopen(bookTickerURL)
            sayfa = response.read().decode("iso-8859-1")
            print(sayfa)
            data_json = json_util.loads(sayfa)
            response.close()
        except:
            status = 0
            print('{} api hatası'.format(exchange))

        for key in data_json:
            value = data_json[key]
            data['_id'] = int(ts)
            data['timestamp'] = d
            temp = key.split('_')
            symbol = temp[1] + temp[0]
            data['symbol'] = symbol
            data['bidPrice'] = data_json[key]['bids'][0][0]
            data['bidQty'] = data_json[key]['bids'][0][1]
            data['askPrice'] = data_json[key]['asks'][0][0]
            data['askQty'] = data_json[key]['asks'][0][1]
            result = db[symbol].insert_one(data)
            dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
            print("{} Inserted_id: {}, {}, {}, {}".format(dFormatted, result.inserted_id, symbol, data['askPrice'], data['bidPrice']))
        print('Poloniex bookTicker done...')
        client.close()
        time.sleep(0.9)
    time.sleep(0.9)

