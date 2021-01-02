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
import pprint

init()
readme = 'https://docs.bitfinex.com/v2/docs/readme'
exchange = 'Yerli borsalar'
bookTickerURL = "https://api.bitfinex.com/v2/tickers?symbols="
btcturk = 'https://www.btcturk.com/api/orderbook?pairSymbol=BTCTRY'

client = MongoClient()
dbbtcturk = client.btcturk_bookTicker

client2 = MongoClient()
dbparibu = client.paribu_bookTicker

client3 = MongoClient()
dbkoineks = client.koineks_bookTicker

client4 = MongoClient()
dbkoinim = client.koinim_bookTicker

btcturkVals = {'id': 0, 'Borsa':'BTCTurk', 'buy':0, 'sell': 0, 'bestBuy': 'false', 'bestSell': 'false'}
paribuVals  = {'id': 1, 'Borsa':'Paribu', 'buy':0, 'sell': 0, 'bestBuy': 'false', 'bestSell': 'false'}
koinimVals  = {'id': 2, 'Borsa':'Koinim', 'buy':0, 'sell': 0, 'bestBuy': 'false', 'bestSell': 'false'}
koineksVals = {'id': 3, 'Borsa':'Koineks', 'buy':0, 'sell': 0, 'bestBuy': 'false', 'bestSell': 'false'}
exchanges = ['Btcturk', 'Paribu', 'Koinim', 'Koineks']
buys  = [0,0,0,0]
sells = [0,0,0,0]

data_json = ''
interval = 10

data_json = {"_id": '', "timestamp": '', "valid": 'false', "error": [], "result": {}}

d = datetime.now(tz=pytz.utc)
dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
print("{} {} kontrol ediliyor...".format(dFormatted, exchange))


def btcturk():
    lastDoc = dbbtcturk['BTCTRY'].find().sort([('_id', -1)]).limit(1)
    result = lastDoc[0]
    btcturkbid = buys[0]  = btcturkVals['buy'] = int(lastDoc[0]['bidPrice'])
    btcturkask = sells[0] = btcturkVals['sell'] = int(lastDoc[0]['askPrice'])
    print('BTCTURK\t{}\t{}'.format(btcturkbid, btcturkask))


def paribu():
    lastDoc = dbparibu['BTCTRY'].find().sort([('_id', -1)]).limit(1)
    result = lastDoc[0]
    paribubid = buys[1] = paribuVals['buy'] = int(lastDoc[0]['bidPrice'])
    paribuask = sells[1] = paribuVals['sell'] = int(lastDoc[0]['askPrice'])
    print('Paribu\t{}\t{}'.format(paribubid, paribuask))


def koinim():
    lastDoc = dbkoinim['BTCTRY'].find().sort([('_id', -1)]).limit(1)
    result = lastDoc[0]
    koinimbid = buys[2] = koinimVals['buy'] = int(lastDoc[0]['bidPrice'])
    koinimask = sells[2] = koinimVals['sell'] = int(lastDoc[0]['askPrice'])
    print('Koinim\t{}\t{}'.format(koinimbid, koinimask))


def koineks():
    lastDoc = dbkoineks['BTCTRY'].find().sort([('_id', -1)]).limit(1)
    result = lastDoc[0]
    koineksbid = buys[3] = koineksVals['buy'] = int(float(lastDoc[0]['bidPrice']))
    koineksask = sells[3] = koineksVals['sell'] = int(float(lastDoc[0]['askPrice']))
    print('Koineks\t{}\t{}'.format(koineksbid, koineksask))

def findBestBuy(mode):
    if mode == 'MARKET':
        return buys.index(max(buys))
    if mode == 'MAKER':
        return buys.index(min(buys))


def findBestSell(mode):
    if mode == 'MARKET':
        return sells.index(min(sells))
    if mode == 'MAKER':
        return sells.index(max(sells))

while True:
    d = datetime.now(tz=pytz.utc)
    ts = time.time()
    valid = 'true'
    if (d.second % interval == 4 ):
        print("{} Interval".format(d.strftime('%d/%m/%Y %H:%M:%S')))
        print("\t{} ...".format(d.strftime('%d/%m/%Y %H:%M:%S')))
        print('Borsa\tBid\tAsk')
        print('\tBuy\tSell')
        btcturk()
        paribu()
        koinim()
        koineks()
        bestBuy = findBestBuy('MARKET')
        bestSell = findBestSell('MARKET')
        fark = int(buys[bestBuy]-sells[bestSell])
        farkYuzde = (fark/int(sells[bestSell]))*100
        farkYuzde = round(farkYuzde,2)
        print('MARKET: Al->{} Sat->{}, Fark TRY: {}, %{}'.format(exchanges[bestSell], exchanges[bestBuy], fark,
                                                                 farkYuzde))

        bestBuy = findBestBuy('MAKER')
        bestSell = findBestSell('MAKER')
        fark = int(sells[bestSell] - buys[bestBuy])
        farkYuzde = (fark / int(buys[bestBuy])) * 100
        farkYuzde = round(farkYuzde, 2)
        print('MAKER: Al->{} Sat->{}, Fark TRY: {}, %{}'.format(exchanges[bestBuy], exchanges[bestSell], fark,
                                                                farkYuzde))
        client.close()
        client2.close()
        client3.close()
        client4.close()

    time.sleep(0.9)
