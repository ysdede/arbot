from pymongo import MongoClient
from datetime import datetime
import pytz, time
from colorama import init
from colorama import Fore, Style
import requests
from vars import ticker_object, mappings, quote_assets
init()

interval = 15
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

        for i in range(len(data_json)):
            symbol = data_json[i]["symbol"]
            
            if not symbol.endswith(tuple(quote_assets[exchange])):
                continue

            bidPrice = float(data_json[i]["bidPrice"])
            bidQty = float(data_json[i]["bidQty"])
            askPrice = float(data_json[i]["askPrice"])
            askQty = float(data_json[i]["askQty"])
            data_json[i]["timestamp"] = d
            data_json[i]["_id"] = int(ts)
            try:
                result = db[symbol].insert_one(data_json[i])
                dFormatted = (
                Fore.GREEN + d.strftime("%d/%m/%Y %H:%M:%S") + Style.RESET_ALL)
                print(f"{dFormatted} Inserted_id: {result.inserted_id}, {symbol}, {askPrice}, {bidPrice}")
                symbol_done += 1
            except Exception as e:
                print(f"MongoDB hatası.\n {e}")

        print(f"Binance bookTicker done. Processed {symbol_done} symbols in {time.time() - start:0.1f} seconds...")
        client.close()
        time.sleep(0.9)
    time.sleep(0.9)
