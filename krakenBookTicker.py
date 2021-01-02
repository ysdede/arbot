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

exchange = 'Kraken'
bookTickerURL = "https://api.kraken.com/0/public/Ticker?pair="
scraper = cfscrape.create_scraper()
client = MongoClient()
db = client.kraken

client2 = MongoClient()
db2 = client.kraken_bookTicker

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

        for pair in result:

            if '^' not in result[pair]['altname']:
                pairsString = pairsString  +  result[pair]['altname'] + ','

        query = bookTickerURL + pairsString[:(len(pairsString)-1)]

        try:
            oldData_json = data_json
            response = scraper.get(query).content
            response = response.replace('DASHXBT','DASHBTC')
            response = response.replace('XXBT', 'BTC')
            response = response.replace('XBT', 'BTC')

            response = response.replace('ZUSD', 'USD')
            response = response.replace('ZCAD', 'CAD')
            response = response.replace('ZGBP', 'GBP')
            response = response.replace('ZJPY', 'JPY')
            response = response.replace('ZEUR', 'EUR')

            response = response.replace('XXLM', 'XLM')
            response = response.replace('XETH', 'ETH')
            response = response.replace('XETC', 'ETC')
            response = response.replace('XLTC', 'LTC')
            response = response.replace('XETH', 'ETH')
            response = response.replace('XXRP', 'XRP')
            response = response.replace('XZEC', 'ZEC')
            response = response.replace('XMLN', 'MLN')
            response = response.replace('XXMR', 'XMR')
            response = response.replace('XXDG', 'DOGE')
            response = response.replace('XICN', 'ICN')
            response = response.replace('XREP', 'REP')
            temp_data_json = json_util.loads(response)

            data_json['error'] = temp_data_json['error']
            data_json['result'] = temp_data_json['result']
        except:
            status = 0
            print('{} api hatası'.format(exchange))
            valid = 'false'

        if 'DASHBTC' not in response:
            valid = 'false'
            data_json = oldData_json
            data_json['valid'] = valid
            dFormatted = Fore.RED + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
            print('Data invalid!')

        insData_json = {"_id":'',"timestamp":'',"valid":'false',"symbol":'',"askPrice":0,"askQty":0, "bidPrice":0,"bidQty":0}

        for pair in data_json['result']:
            symbol = pair
            insData_json['symbol'] = pair
            insData_json['timestamp'] = d
            insData_json['_id'] = int(ts)
            insData_json['valid'] = valid

            insData_json['askPrice'] = askPrice = data_json['result'][pair]['a'][0]
            insData_json['askQty']   = askQty   = data_json['result'][pair]['a'][1]

            insData_json['bidPrice'] = bidPrice = data_json['result'][pair]['b'][0]
            insData_json['bidQty']   = bidQty   = data_json['result'][pair]['b'][1]

            result = db2[symbol].insert_one(insData_json)
            dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
            print("{} Inserted_id: {}, {}, {}, {}, {}, {}".format(dFormatted, result.inserted_id, symbol, askPrice, askQty, bidPrice, bidQty))

        print('{} BookTicker done...'.format(exchange))
        oldData_json = data_json
        client.close()
        client2.close()
        time.sleep(0.9)
    time.sleep(0.9)