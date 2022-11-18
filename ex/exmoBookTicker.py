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

exchange = 'Exmo'
bookTickerURL = "https://api.exmo.com/v1/ticker/"
scraper = cfscrape.create_scraper()
client = MongoClient()
db = client.exmo_bookTicker
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
            response = response.replace('buy_price', 'bidPrice')
            response = response.replace('sell_price', 'askPrice')
            response = response.replace('last_trade', 'last')
            response = response.replace('_', '')
            oldData_json = data_json
            data_json = json_util.loads(response)
        except:
            print('{} api hatası'.format(exchange))

        if 'BTCUSD' not in response:
            print('Hata, önceki veriler kaydediliyor...')
            data_json = oldData_json
            valid = 'false'

        for item in data_json:
            symbol = item
            bidPrice = data_json[symbol]['bidPrice']
            askPrice = data_json[symbol]['askPrice']
            data_json[symbol]['symbol'] = symbol
            data_json[symbol]['timestamp'] = d
            data_json[symbol]['_id'] = int(ts)
            data_json[symbol]['valid'] = valid

            result = db[symbol].insert_one(data_json[symbol])
            dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
            print(
                "{} Inserted_id: {}, {}, {}, {}".format(dFormatted, result.inserted_id, symbol, askPrice, bidPrice))

        oldData_json = data_json
        print('{} bookTicker done...'.format(exchange))
        client.close()
        time.sleep(0.9)

    time.sleep(0.9)
