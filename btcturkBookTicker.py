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

exchange = 'BTCTurk'
bookTickerURL = "https://www.btcturk.com/api/ticker"
scraper = cfscrape.create_scraper()
client = MongoClient()
db = client.btcturk_bookTicker
data_json = ''
interval = 30

d = datetime.now(tz=pytz.utc)
dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
print("{} {} bookTicker started...".format(dFormatted, exchange))

while True:
    d = datetime.now(tz=pytz.utc)
    ts = time.time()
    if (not d.second % interval):
        print ("{} Interval".format(d.strftime('%d/%m/%Y %H:%M:%S')))
        try:
            response = scraper.get(bookTickerURL).content
            oldData_json = data_json
            response = response.replace('pair', 'symbol')
            response = response.replace('bid', 'bidPrice')
            response = response.replace('ask', 'askPrice')
            data_json = json_util.loads(response)

        except:
            status = 0
            print('{} api hatası'.format(exchange))

        if 'error' in response:
            for i in range(len(oldData_json)):
                data_json = oldData_json
                data_json[i]['valid'] = 'false'

        for i in range(len(data_json)):
            symbol = data_json[i]['symbol']
            bidPrice = data_json[i]['bidPrice']
            askPrice = data_json[i]['askPrice']
            data_json[i]['timestamp'] = d
            data_json[i]['_id'] = int(ts)
            data_json[i]['valid'] = 'true'
            result = db[symbol].insert_one(data_json[i])
            dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
            print(
                "{} Inserted_id: {}, {}, {}, {}".format(dFormatted, result.inserted_id, symbol, askPrice, bidPrice))

        print('{} bookTicker done...'.format(exchange))
        client.close()
        time.sleep(0.9)

    time.sleep(0.9)
