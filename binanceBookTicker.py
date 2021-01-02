# -*- coding: utf8 -*-
from __future__ import print_function
from __future__ import division
from pymongo import MongoClient
from datetime import datetime
import pytz, time
from colorama import init
from colorama import Fore, Style
import requests

init()

interval = 30

client = MongoClient()
db = client.binance_bookTicker
bookTickerURL = """https://api.binance.com/api/v3/ticker/bookTicker"""

d = datetime.now(tz=pytz.utc)
dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
print("{} Binance bookTicker started...".format(dFormatted))

while True:
    d = datetime.now(tz=pytz.utc)
    ts = time.time()
    if(not d.second % interval):
        print ("{} Interval".format(d.strftime('%d/%m/%Y %H:%M:%S')))
        try:
            response = requests.get(bookTickerURL)
            print(response)
            sayfa = response
            print(sayfa)
            data_json = response.json()
        except:
            status = 0
            print('Binance api hatası')

        for i in range(len(data_json)):
            symbol = data_json[i]['symbol']
            bidPrice = data_json[i]['bidPrice']
            bidQty = data_json[i]['bidQty']
            askPrice = data_json[i]['askPrice']
            askQty = data_json[i]['askQty']
            data_json[i]['timestamp'] = d
            data_json[i]['_id'] = int(ts)
            result = db[symbol].insert_one(data_json[i])
            print(result)
            dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
            print("{} Inserted_id: {}, {}, {}, {}".format(dFormatted, result.inserted_id, symbol, askPrice, bidPrice))

        print('Binance bookTicker done...')
        client.close()
        time.sleep(0.9)
    time.sleep(0.9)