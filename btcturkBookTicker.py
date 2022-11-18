from pymongo import MongoClient
from bson import json_util
from datetime import datetime
import pytz, time
import cfscrape
from colorama import init
from colorama import Fore, Back, Style
import requests
from vars import ticker_object, mappings, quote_assets, quote_blacklist, ticker_interval
init()

exchange = 'btcturk'
bookTickerURL = "https://api.btcturk.com/api/v2/ticker"
scraper = cfscrape.create_scraper()
client = MongoClient()
db = client.btcturk_bookTicker
data_json = ''
interval = ticker_interval

d = datetime.now(tz=pytz.utc)
dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
print(f"{dFormatted} {exchange} bookTicker started...")

while True:
    d = datetime.now(tz=pytz.utc)
    ts = time.time()
    if (not d.second % interval):
        start = time.time()
        symbol_done = 0
        record = ticker_object.copy()
        print(f"{d.strftime('%d/%m/%Y %H:%M:%S')} Interval")
        try:
            # response = scraper.get(bookTickerURL).content
            # response = scraper.get(bookTickerURL).content.decode("utf-8")
            response = requests.get(bookTickerURL).json()
            # print(response, type(response))
            oldData_json = data_json
            # response = response.replace('pair', 'symbol')
            # response = response.replace('bid', 'bidPrice')
            # response = response.replace('ask', 'askPrice')
            data_json = response  # json_util.loads(response)

        except Exception as e:
            status = 0
            print(f'{exchange} api hatası.\n{e}')

        if 'error' in response:
            for i in range(len(oldData_json)):
                data_json = oldData_json
                data_json[i]['valid'] = 'false'

        if 'success' not in response or response['success'] == False:
            print('Hata...')
            continue

        for sym in data_json['data']:
            # print(sym)
            record['symbol'] = sym[mappings[exchange]['symbol']]

            if not record['symbol'].endswith(tuple(quote_assets[exchange])) or record['symbol'].endswith(tuple(quote_blacklist[exchange])):
                continue

            record['bidPrice'] = float(sym[mappings[exchange]['bidPrice']])
            record['askPrice'] = float(sym[mappings[exchange]['askPrice']])
            record['timestamp'] = d  # TODO: !
            record['_id'] = int(ts)
            record['valid'] = 'true'
            # print(f"------------------\n{record}\n------------------")
            try:
                result = db[record['symbol']].insert_one(record)
                dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
                print(f"{dFormatted} Inserted_id: {result.inserted_id}, {record['symbol']}, {record['askPrice']}, {record['bidPrice']}")
                symbol_done += 1
            except Exception as e:
                print(f"{d.strftime('%d/%m/%Y %H:%M:%S')} {e}")
                continue
            
            
            # symbol = data_json[i]['symbol']
            # bidPrice = data_json[i]['bidPrice']
            # askPrice = data_json[i]['askPrice']
            # data_json[i]['timestamp'] = d
            # data_json[i]['_id'] = int(ts)
            # data_json[i]['valid'] = 'true'
            # result = db[symbol].insert_one(data_json[i])
        print(f"{exchange} bookTicker done. Processed {symbol_done} symbols in {time.time() - start:0.1f} seconds...")
        client.close()
        time.sleep(0.9)

    time.sleep(0.9)
