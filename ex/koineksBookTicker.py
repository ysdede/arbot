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

exchange = 'Koineks'
bookTickerURL = "https://koineks.com/ticker"
scraper = cfscrape.create_scraper()
client = MongoClient()
db = client.koineks_bookTicker
data_json = ''
interval = 30

d = datetime.now(tz=pytz.utc)
dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
print("{} {} bookTicker started...".format(dFormatted, exchange))

while True:
    d = datetime.now(tz=pytz.utc)
    ts = time.time()
    valid = 'true'
    if (not d.second % interval):
        print ("{} Interval".format(d.strftime('%d/%m/%Y %H:%M:%S')))
        try:
            response = scraper.get(bookTickerURL).content
            response = response.replace('short_code', 'symbol')
            response = response.replace('ask', 'askPrice')
            response = response.replace('bid', 'bidPrice')
            oldData_json = data_json
            data_json = json_util.loads(response)

        except:
            print('{} api hatası'.format(exchange))

        if 'symbol' not in response:
            print('Hata, önceki veriler kaydediliyor...')
            data_json = oldData_json
            valid = 'false'

        for sc in data_json:
            symbol = sc + data_json[sc]['currency']
            bidPrice = data_json[sc]['bidPrice']
            askPrice = data_json[sc]['askPrice']
            data_json[sc]['symbol'] = symbol
            data_json[sc]['timestamp'] = d
            data_json[sc]['_id'] = int(ts)
            data_json[sc]['valid'] = valid
            result = db[symbol].insert_one(data_json[sc])
            dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
            print(
                "{} Inserted_id: {}, {}, {}, {}".format(dFormatted, result.inserted_id, symbol, askPrice, bidPrice))

        oldData_json = data_json
        print('{} bookTicker done...'.format(exchange))
        client.close()

        time.sleep(0.9)

    time.sleep(0.9)
