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

exchange = 'Paribu'
bookTickerURL = "https://www.paribu.com/ticker"
scraper = cfscrape.create_scraper()
client = MongoClient()
db = client.paribu_bookTicker
data_json = ''
interval = 30

shortCodes = ['BTC_TL']

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
            response = response.replace('highestBid', 'bidPrice')
            response = response.replace('lowestAsk', 'askPrice')
            oldData_json = data_json
            data_json = json_util.loads(response)

        except:
            print('{} api hatası'.format(exchange))

        if b'bidPrice' not in response:
            print('Hata, önceki veriler kaydediliyor...')
            data_json = oldData_json
            valid = 'false'

        for code in shortCodes:
            symbol = code.replace('_TL', 'TRY')
            bidPrice = data_json[code]['bidPrice']
            askPrice = data_json[code]['askPrice']
            data_json[code]['timestamp'] = d
            data_json[code]['_id'] = int(ts)
            data_json[code]['valid'] = valid
            data_json[code]['symbol'] = symbol
            result = db[symbol].insert_one(data_json[code])
            dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
            print(
                "{} Inserted_id: {}, {}, {}, {}".format(dFormatted, result.inserted_id, symbol, askPrice, bidPrice))

        print('{} bookTicker done...'.format(exchange))
        client.close()
        time.sleep(0.9)

    time.sleep(0.9)

