# -*- coding: utf8 -*-
from __future__ import print_function
from __future__ import division
from pymongo import MongoClient
from bson import json_util
from datetime import datetime
import pytz, time
from colorama import init
from colorama import Fore, Back, Style
import cfscrape
from bs4 import BeautifulSoup
import requests
init()

interval = 30

client = MongoClient()
db = client.koinim_bookTicker
scraper = cfscrape.create_scraper()


dataLastLTC = {"_id":"0123456789", "timestamp":"1234567890","symbol": "LTCTRY", "last": "0.9"}
dataLastBTC = {"_id":"0123456789", "timestamp":"1234567890","symbol": "BTCTRY", "last": "0.9"}

bookTickerURL = """https://koinim.com/ticker"""
indexURL = """https://koinim.com"""

d = datetime.now(tz=pytz.utc)
dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
print("{} Koinim bookTicker started...".format(dFormatted))

while True:
    d = datetime.now(tz=pytz.utc)
    ts = time.time()
    if(not d.second % interval):
        print ("{} Interval".format(d.strftime('%d/%m/%Y %H:%M:%S')))

        try:
            response = scraper.get(bookTickerURL).content
            response = response.replace('bid', 'bidPrice')
            response = response.replace('ask', 'askPrice')
            response = response.replace('last_order', 'last')

            data_json = json_util.loads(response)
        except:
            print(response)
            print('Koinim api hatası')

        symbol = 'BTCTRY'
        bidPrice = data_json['bidPrice']
        askPrice = data_json['askPrice']

        data_json['timestamp'] = dataLastBTC['timestamp'] = dataLastLTC['timestamp'] =  d
        data_json['_id'] = dataLastBTC['_id'] = dataLastLTC['_id'] = int(ts)

        result = db[symbol].insert_one(data_json)
        dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
        print("{} Inserted_id: {}, {}, {}, {}".format(dFormatted, result.inserted_id, symbol, askPrice, bidPrice))
        print('Koinim bookTicker done...')

        response = requests.get(indexURL)

        if 'nav-BTC-price' not in response.text:
            print('HTML Scrapping hatası, eski veriler tekrarlanıyor...')
            dataLastLTC['valid'] = 'false'
            dataLastBTC['valid'] = 'false'
            soup = oldSoup
        else:
            soup = BeautifulSoup(response.text, 'html.parser')
            dataLastLTC['valid'] = 'true'
            dataLastBTC['valid'] = 'true'
            oldSoup = soup

        mydiv = soup.find("strong", class_="nav-BTC-price")
        dataLastBTC['last'] = mydiv.contents[0]

        mydiv = soup.find("strong", class_="nav-LTC-price")
        dataLastLTC['last'] = mydiv.contents[0] = mydiv.contents[0]

        result = db['BTCTRYLast'].insert_one(dataLastBTC)
        dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
        print("{} Inserted_id: {}, {}, {}".format(dFormatted, result.inserted_id, 'BTCTRY', dataLastBTC['last']))

        result = db['LTCTRYLast'].insert_one(dataLastLTC)
        dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
        print("{} Inserted_id: {}, {}, {}".format(dFormatted, result.inserted_id, 'LTCTRY', dataLastLTC['last']))
        print('Koinim index scrapping done...')

        time.sleep(0.9)
        client.close()
    time.sleep(0.9)