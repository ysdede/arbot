from pymongo import MongoClient
from bson import json_util
from datetime import datetime
import pytz, time
import cfscrape
from colorama import init
from colorama import Fore, Back, Style
from vars import ticker_object, mappings, quote_assets, quote_blacklist, ticker_interval
from utils import check_rate_limit, utc_ts_now_ms, store_last_api_call_ts, ts_strf, fmt_paribu_symbol
init()

exchange = "paribu"
bookTickerURL = "https://www.paribu.com/ticker"
scraper = cfscrape.create_scraper()
client = MongoClient()
db = client.paribu_bookTicker
data_json = ""
interval = ticker_interval

d = datetime.now(tz=pytz.utc)
dFormatted = Fore.GREEN + d.strftime("%d/%m/%Y %H:%M:%S") + Style.RESET_ALL
print(f"{dFormatted} {exchange} bookTicker started...")

while True:
    d = datetime.now(tz=pytz.utc)
    d_str = d.strftime("%d/%m/%Y %H:%M:%S")
    ts = d.timestamp()
    valid = "true"
    if not d.second % interval:
        start = time.time()
        symbol_done = 0
        record = ticker_object.copy()
        print(f'{d_str} Tick!')

        while not check_rate_limit(exchange):
            # print(f'{d_str} Rate limit reached. Waiting...')
            time.sleep(0.1)

        try:
            api_call_ts = utc_ts_now_ms()  # round(datetime.now(tz=pytz.utc).timestamp() * 1000)
            response = scraper.get(bookTickerURL).content.decode("utf-8")
            # print(api_call_ts, ts_strf(api_call_ts))
            store_last_api_call_ts(exchange, api_call_ts, db.name)
        except Exception as e:
            print(f"{exchange} api hatası.\n {e}")

        oldData_json = data_json
        data_json = json_util.loads(response)
        # print(response, type(response))
        # print(data_json, type(data_json))

        if "highestBid" not in response:
            print("Hata, önceki veriler kaydediliyor...")
            data_json = oldData_json
            valid = "false"

        for k, v in data_json.items():
            record["symbol"] = fmt_paribu_symbol(k)
            
            if not record['symbol'].endswith(tuple(quote_assets[exchange])) or record['symbol'].endswith(tuple(quote_blacklist[exchange])):
                print(f"{d_str} {record['symbol']} geçersiz.")
                continue

            record['bidPrice'] = float(v[mappings[exchange]['bidPrice']])
            record['askPrice'] = float(v[mappings[exchange]['askPrice']])
            record['timestamp'] = d  # TODO: !
            record['_id'] = int(ts)
            record['valid'] = valid
            record['baseAsset'] =k.split("_")[0]
            record['quoteAsset'] =k.split("_")[1]
            
            try:
                result = db[record['symbol']].insert_one(record)
                dFormatted = f"{Fore.GREEN}{d_str}{Style.RESET_ALL}"
                print(f"{dFormatted} Inserted_id: {result.inserted_id}, {record['symbol']}, {record['askPrice']}, {record['bidPrice']}")
                symbol_done += 1
            except Exception as e:
                print(f"{d_str} {e}")
                continue

        # print(f"{exchange} bookTicker done...")
        print(f"{exchange} bookTicker done. Processed {symbol_done} symbols in {time.time() - start:0.1f} seconds...")
        client.close()
        time.sleep(0.9)

    time.sleep(0.9)
