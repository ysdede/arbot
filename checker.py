# -*- coding: utf8 -*-
from __future__ import print_function
from __future__ import division
from pymongo import MongoClient
from colorama import init
import sys

init()

def collect(pair, dbs):
    tmpclient = MongoClient()
    result = []
    for dbname in dbs:
        print('Connecting db: {}'.format(dbname))
        tmpdb  = tmpclient[dbname]
        colls = tmpdb.collection_names()
        if pair in colls:
            exchanges = []
            print('Found pair {}@:{}'.format(dbname, pair))
            lastDoc = tmpdb[pair].find().sort([('_id', -1)]).limit(1)
            times = lastDoc[0]['timestamp'].strftime('%d/%m/%Y %H:%M:%S')
            bid = lastDoc[0]['bidPrice']
            ask = lastDoc[0]['askPrice']
            print(times, bid, ask)
            exchanges = [dbname, times, bid, ask]
            result.append(exchanges)
    return result


print('sys.argv: ', sys.argv)

if len(sys.argv) > 1:
    pair = sys.argv[1].upper()

print('pair: ', pair)

booktickerdbs = []

client = MongoClient()
dblist = client.list_database_names()

print(dblist)

for dbs in dblist:
    if 'bookTicker' in dbs:
        booktickerdbs.append(dbs)

print(booktickerdbs)

client.close()

res = collect(pair, booktickerdbs)

print('exc\ttimes\tbid\task')

for vals in res:
    exc = vals[0].replace('_bookTicker', '')
    times = vals[1]
    bid = vals[2]
    ask = vals[3]
    print('{}\t{}\t{}\t{}'.format(times, bid, ask, exc))

