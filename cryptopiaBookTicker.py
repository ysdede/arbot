# -*- coding: utf8 -*-
from __future__ import print_function
from __future__ import division
from pymongo import MongoClient
from bson import json_util
from datetime import datetime
import pytz, time
import cfscrape
from colorama import init
from colorama import Fore, Back, Style

init()

exchange = 'Cryptopia'
bookTickerURL = "https://www.cryptopia.co.nz/api/GetMarkets"
scraper = cfscrape.create_scraper()
client = MongoClient()
db = client.cryptopia_bookTicker
data_json = ''
interval = 30

result = {}
d = datetime.now(tz=pytz.utc)
dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
print("{} {} bookTicker started...".format(dFormatted, exchange))

while True:
    d = datetime.now(tz=pytz.utc)
    ts = time.time()
    valid = 'true'
    if (not d.second % interval):
        print("{} Interval".format(d.strftime('%d/%m/%Y %H:%M:%S')))
        try:
            response = scraper.get(bookTickerURL).content
            response = response.replace('Volume', 'volume')
            response = response.replace('Sellvolume', 'sellVolume')
            response = response.replace('Basevolume', 'baseVolume')
            response = response.replace('Buyvolume', 'buyVolume')
            response = response.replace('BuybaseVolume', 'buyBaseVolume')
            response = response.replace('SellbaseVolume', 'sellBaseVolume')
            response = response.replace('LastPrice', 'lastPrice')
            response = response.replace('TradePairId', 'tradePairId')
            response = response.replace('Label', 'symbol')
            response = response.replace('High', 'high')
            response = response.replace('BidPrice', 'bidPrice')
            response = response.replace('Low', 'low')
            response = response.replace('Close', 'close')
            response = response.replace('Open', 'open')
            response = response.replace('AskPrice', 'askPrice')
            response = response.replace('Change', 'change')

            oldData_json = data_json
            data_json = json_util.loads(response)
        except:
            print('{} api hatası'.format(exchange))

        if """"Success":true""" not in response:
            print('Hata, önceki veriler kaydediliyor...')
            data_json = oldData_json
            valid = 'false'

        for pair in data_json['Data']:
            symbol = pair['symbol'].replace('/', '')
            symbol = symbol.replace('$', '^')
            pair['symbol'] = symbol
            bidPrice = pair['bidPrice']
            askPrice = pair['askPrice']
            pair['timestamp'] = d
            pair['_id'] = int(ts)
            pair['valid'] = valid
            result = db[symbol].insert_one(pair)
            dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
            print(
                "{} Inserted_id: {}, {}, {}, {}".format(dFormatted, result.inserted_id, symbol, askPrice, bidPrice))

        oldData_json = data_json
        print('{} bookTicker done...'.format(exchange))
        client.close()
        time.sleep(0.9)

    time.sleep(0.9)
