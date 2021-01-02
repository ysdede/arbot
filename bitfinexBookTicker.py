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
readme = 'https://docs.bitfinex.com/v2/docs/readme'
exchange = 'Bitfinex'
bookTickerURL = "https://api.bitfinex.com/v2/tickers?symbols="
scraper = cfscrape.create_scraper()

client = MongoClient()
db = client.bitfinex

client2 = MongoClient()
db2 = client.bitfinex_bookTicker

data_json = ''
interval = 30

data_json = {"_id":'',"timestamp":'',"valid":'false',"error":[],"result":{}}

d = datetime.now(tz=pytz.utc)
dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
print("{} {} bookTicker started...".format(dFormatted, exchange))

while True:
    d = datetime.now(tz=pytz.utc)
    ts = time.time()
    valid = 'true'

    if (not d.second % interval):
        print ("{} Interval".format(d.strftime('%d/%m/%Y %H:%M:%S')))
        print("\t{} Fetching bookTicker...".format(d.strftime('%d/%m/%Y %H:%M:%S')))

        lastDoc = db['assetPairs'].find().sort([('_id', -1)]).limit(1)

        result = lastDoc[0]['result']
        pairsString = ''

        for item in result:
            pairStr = item['pair']
            pairStr = 't' + pairStr.upper()
            pairsString = pairsString + pairStr + ','
        query = bookTickerURL + pairsString[:(len(pairsString)-1)]

        try:
            response = scraper.get(query).content
            response = str(response).decode("iso-8859-1")

            response = response.replace('DSHBTC', 'DASHBTC')
            response = response.replace('DSHUSD', 'DASHUSD')
            temp_data_json = json_util.loads(response)
            data_json = temp_data_json
        except:
            status = 0
            print('{} api hatası!'.format(exchange))
            valid = 'false'

        if 'tBTCUSD' not in response:
            valid = 'false'
            data_json = oldData_json
            dFormatted = Fore.RED + datetime.now(tz=pytz.utc).strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
            print('Veri hatası, eski veriler kaydediliyor...!')

        insData_json = {"_id":'',"timestamp":'',"valid":'false',"symbol":'',"askPrice":0,"askQty":0, "bidPrice":0,"bidQty":0}

        for item in data_json:
            symbol = item[0]
            symbol = symbol[1:]

            insData_json['symbol'] = symbol
            insData_json['timestamp'] = d
            insData_json['_id'] = int(ts)
            insData_json['valid'] = valid

            insData_json['askPrice'] = askPrice = item[3]
            insData_json['askQty']   = askQty   = item[4]

            insData_json['bidPrice'] = bidPrice = item[1]
            insData_json['bidQty']   = bidQty   = item[2]

            result = db2[symbol].insert_one(insData_json)
            dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
            print("{} Inserted_id: {}, {}, {}, {}, {}, {}".format(dFormatted, result.inserted_id, symbol, askPrice, askQty, bidPrice, bidQty))

        print('{} bookticker done...'.format(exchange))
        oldData_json = data_json
        client.close()
        client2.close()
        time.sleep(0.9)
    time.sleep(0.9)