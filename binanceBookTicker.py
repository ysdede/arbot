from pymongo import MongoClient
from datetime import datetime
import pytz, time
from colorama import init
from colorama import Fore, Style
import requests
from vars import ticker_object, mappings, quote_assets, quote_blacklist, ticker_interval
init()

interval = ticker_interval
# quoteAssets = ["TRY", "USDT"]
exchange = 'binance'
client = MongoClient()
db = client.binance_bookTicker
bookTickerURL = """https://api.binance.com/api/v3/ticker/bookTicker"""

d = datetime.now(tz=pytz.utc)
dFormatted = Fore.GREEN + d.strftime("%d/%m/%Y %H:%M:%S") + Style.RESET_ALL
print(f"{dFormatted} Binance bookTicker started...")

while True:
    d = datetime.now(tz=pytz.utc)
    ts = time.time()
    if not d.second % interval:
        start = time.time()
        record = ticker_object.copy()
        symbol_done = 0
        print(f'{d.strftime("%d/%m/%Y %H:%M:%S")} Interval')
        try:
            response = requests.get(bookTickerURL)
            data_json = response.json()
        except Exception as e:
            status = 0
            print(f"Binance api hatası.\n {e}")

        for sym in data_json:
            record['symbol'] = sym[mappings[exchange]['symbol']]
            
            if not record['symbol'].endswith(tuple(quote_assets[exchange])) or record['symbol'].endswith(tuple(quote_blacklist[exchange])):
                continue
            
            record['bidPrice'] = float(sym[mappings[exchange]['bidPrice']])
            record['askPrice'] = float(sym[mappings[exchange]['askPrice']])
            record['timestamp'] = d  # TODO: !
            record['_id'] = int(ts)
            record['valid'] = 'true'

            try:
                result = db[record['symbol']].insert_one(record)
                dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
                print(f"{dFormatted} Inserted_id: {result.inserted_id}, {record['symbol']}, {record['askPrice']}, {record['bidPrice']}")
                symbol_done += 1
            except Exception as e:
                print(f"{d.strftime('%d/%m/%Y %H:%M:%S')} {e}")
                continue

        print(f"{exchange} bookTicker done. Processed {symbol_done} symbols in {time.time() - start:0.1f} seconds...")
        client.close()
        time.sleep(0.9)
    time.sleep(0.9)
