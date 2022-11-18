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

exchange = 'Kucoin'
bookTickerURL = "https://api.kucoin.com/api/v1/market/allTickers"
scraper = cfscrape.create_scraper()
client = MongoClient()
db = client.kucoin_bookTicker
data_json = ''
interval = 30


data_json = {"_id":'',"timestamp":'',"valid":'false',"success":'',"code":'',"msg":'',"data":[]}

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
            print(response)
            response = response.replace('lastDealPrice', 'last')
            response = response.replace('buy', 'bidPrice')
            response = response.replace('sell', 'askPrice')
            response = response.replace('datetime', 'timestamp')

            oldData_json = data_json
            data_json = json_util.loads(response)
        except:
            print('{} api hatası'.format(exchange))


        if b"""code": "200000""" not in response:
            print('Hata, önceki veriler kaydediliyor...')
            data_json = oldData_json
            valid = 'false'

        for pair in data_json['data']:

            symbol = pair['symbol'].replace('-', '')
            pair['symbol'] = symbol

            try:
                bidPrice = pair['bidPrice']
            except:
                print('Missing bid price: {}'.format(symbol))
                pair['bidPrice'] = -1
                bidPrice = pair['bidPrice']

            try:
                askPrice = pair['askPrice']
            except:
                print('Missing ask price: {}'.format(symbol))
                pair['askPrice'] = -1
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
