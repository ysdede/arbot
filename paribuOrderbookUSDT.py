from pymongo import MongoClient
from bson import json_util
import json
from datetime import datetime
import pytz, time
import cfscrape
from colorama import init
from colorama import Fore, Back, Style
from vars import ticker_object, mappings, quote_assets, quote_blacklist, ticker_interval, orderbook_interval, paribu_rate_limit, orderbook_object
from utils import check_rate_limit, utc_ts_now_ms, store_last_api_call_ts, ts_strf, fmt_paribu_symbol

init()

exchange = "paribu"
orderbookURL = "https://v3.paribu.com/app/markets/"
scraper = cfscrape.create_scraper()
client = MongoClient()
db = client.paribu_orderbook
data_json = ""
interval = orderbook_interval

d = datetime.now(tz=pytz.utc)
dFormatted = Fore.GREEN + d.strftime("%d/%m/%Y %H:%M:%S") + Style.RESET_ALL
print(f"{dFormatted} {exchange} orderbook started...")

def get_orderbook(symbol):
    url = f'{orderbookURL}{symbol}'

    while not check_rate_limit(exchange):
            # print(f'{d_str} Rate limit reached. Waiting...')
            time.sleep(0.1)
    try:
        print(url)
        api_call_ts = utc_ts_now_ms()  # round(datetime.now(tz=pytz.utc).timestamp() * 1000)
        response = scraper.get(url).content.decode("utf-8")
        store_last_api_call_ts(exchange, api_call_ts, db.name)
        # print(response, type(response))
    except Exception as e:
        print(f"{exchange} api hatası.\n {e}\n {url}")
    
    data_json = json.loads(response)

    if 'success' not in response or data_json['success'] == False:
        print(f'Hata. {url} {response}')
        return None
    else:
        print(f"Succes: {data_json['success']}")
    # print(data_json)
    return *parse_orderbook(data_json), int(api_call_ts / 1000)

    # print(data_json, type(data_json))

def parse_orderbook(data_json):
    sells, buys = [], []

    for k, v in data_json['data']['orderBook']['sell'].items():
        sells.append({'price': float(k), 'qty': float(v)})
    
    for k, v in data_json['data']['orderBook']['buy'].items():
        buys.append({'price': float(k), 'qty': float(v)})

    market_matches = data_json['data']['marketMatches']

    for i in range(len(market_matches)):
        market_matches[i]['price'] = float(market_matches[i]['price'])
        market_matches[i]['amount'] = float(market_matches[i]['amount'])

    print(sells, type(sells))
    return sells, buys, market_matches

symbol = "usdt-tl"
n = 3

while True:
    d = datetime.now(tz=pytz.utc)
    d_str = d.strftime("%d/%m/%Y %H:%M:%S")
    ts = d.timestamp()
    valid = "true"
    if not d.second % interval:
        start = time.time()
        symbol_done = 0
        record = orderbook_object.copy()
        print(f'{d.strftime("%d/%m/%Y %H:%M:%S")} Interval')

        sells, buys, matches, ts = get_orderbook(symbol)

        record["symbol"] = fmt_paribu_symbol(symbol)

        if not record['symbol'].endswith(tuple(quote_assets[exchange])) or record['symbol'].endswith(tuple(quote_blacklist[exchange])):
            continue
        
        record["timestamp"] = datetime.fromtimestamp(ts, tz=pytz.utc)
        record['_id'] = int(ts)
        record['valid'] = valid
        record['sells'] = sells
        record['buys'] = buys
        record['matches'] = matches

        # print(record)
        # exit()
        
        # calculate sum of all sells and buys
        # record['sells_sum'] = sum([x['qty'] for x in record['sells']])
        # record['buys_sum'] = sum([x['qty'] for x in record['buys']])
        # record['matches_sum'] = sum([x['amount'] for x in record['matches']])
        
        sells_qty_sum = 0
        buys_qty_sum = 0
        sells_avg_price = 0
        buys_avg_price = 0
        sells_value = 0
        buys_value = 0
        matches_qty_sum = 0
        matches_value = 0
        matches_avg_price = 0

        for i in range(0, n):
            # print(i, record['sells'][i]['qty'], record['sells'][i]['price'])
            sells_value += record['sells'][i]['qty'] * record['sells'][i]['price']
            buys_value += record['buys'][i]['qty'] * record['buys'][i]['price']
            matches_value += record['matches'][i]['amount'] * record['matches'][i]['price']

            sells_qty_sum += record['sells'][i]['qty']
            buys_qty_sum += record['buys'][i]['qty']
            matches_qty_sum += record['matches'][i]['amount']

            sells_average_price = sells_value / sells_qty_sum
            buys_average_price = buys_value / buys_qty_sum
            matches_average_price = matches_value / matches_qty_sum
        
        record['sells_qty_sum'] = round(sells_qty_sum, 5)
        record['buys_qty_sum'] = round(buys_qty_sum, 5)
        record['sells_average_price'] = round(sells_average_price, 3)
        record['buys_average_price'] = round(buys_average_price, 3)
        record['sells_value'] = round(sells_value, 3)
        record['buys_value'] = round(buys_value, 3)
        record['matches_qty_sum'] = round(matches_qty_sum, 5)
        record['matches_value'] = round(matches_value, 3)
        record['matches_average_price'] = round(matches_average_price, 3)
        record['n'] = n

        try:
            result = db[record['symbol']].insert_one(record)
            dFormatted = f"{Fore.GREEN}{d_str}{Style.RESET_ALL}"
            symbol_done += 1
        except Exception as e:
            print(f"{d_str} Error! {e}")
            continue

        # print(record)

        print(f"{dFormatted} Inserted_id: {result.inserted_id}, {record['symbol']}, avg. sell: {record['sells_average_price']} (qty: {sells_qty_sum:0.0f}), avg buy: {record['buys_average_price']} (qty: {buys_qty_sum:0.0f}), matches avg price: {matches_average_price:0.03f} (qty: {matches_qty_sum:0.0f}) done in {round(time.time() - start, 2)} seconds")

        # try:
        #     response = scraper.get(bookTickerURL).content.decode("utf-8")
        # except Exception as e:
        #     print(f"{exchange} api hatası.\n {e}")

        # oldData_json = data_json
        # data_json = json_util.loads(response)
        # # print(response, type(response))
        # print(data_json, type(data_json))

        # if "highestBid" not in response:
        #     print("Hata, önceki veriler kaydediliyor...")
        #     data_json = oldData_json
        #     valid = "false"

        # for k, v in data_json.items():
        #     record["symbol"] = k
            
        #     if not record['symbol'].endswith(tuple(quote_assets[exchange])) or record['symbol'].endswith(tuple(quote_blacklist[exchange])):
        #         continue

        #     record["symbol"] = record["symbol"].replace("_TL", "TRY")
        #     record["symbol"] = record["symbol"].replace("_USDT", "USDT")
        #     record['bidPrice'] = float(v[mappings[exchange]['bidPrice']])
        #     record['askPrice'] = float(v[mappings[exchange]['askPrice']])
        #     record['timestamp'] = d  # TODO: !
        #     record['_id'] = int(ts)
        #     record['valid'] = valid
        #     record['baseAsset'] =k.split("_")[0]
        #     record['quoteAsset'] =k.split("_")[1]
            
        #     try:
        #         result = db[record['symbol']].insert_one(record)
        #         dFormatted = Fore.GREEN + d.strftime('%d/%m/%Y %H:%M:%S') + Style.RESET_ALL
        #         print(f"{dFormatted} Inserted_id: {result.inserted_id}, {record['symbol']}, {record['askPrice']}, {record['bidPrice']}")
        #         symbol_done += 1
        #     except Exception as e:
        #         print(f"{d.strftime('%d/%m/%Y %H:%M:%S')} {e}")
        #         continue

        # # print(f"{exchange} bookTicker done...")
        # print(f"{exchange} bookTicker done. Processed {symbol_done} symbols in {time.time() - start:0.1f} seconds...")
        client.close()
        time.sleep(0.9)

    time.sleep(0.9)
