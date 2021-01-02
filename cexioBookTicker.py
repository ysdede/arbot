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

exchange = 'Cexio'
bookTickerURL = "https://cex.io/api/tickers/USD/EURO/GBP/RUB/BTC"
scraper = cfscrape.create_scraper()
client = MongoClient()
db = client.cexio_bookTicker
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
            response = response.replace('pair', 'symbol')
            response = response.replace('bid', 'bidPrice')
            response = response.replace('ask', 'askPrice')

            oldData_json = data_json
            data_json = json_util.loads(response)
        except:
            print('{} api hatası'.format(exchange))

        if 'BTC:USD' not in response:
            print('Hata, önceki veriler kaydediliyor...')
            data_json = oldData_json
            valid = 'false'

        for pair in data_json['data']:
            symbol = pair['symbol'].replace(':', '')
            pair['symbol'] = symbol
            bidPrice = pair['bidPrice']
            # bidQty = data_json[i]['bidQty']
            askPrice = pair['askPrice']
            # askQty = data_json[i]['askQty']
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
