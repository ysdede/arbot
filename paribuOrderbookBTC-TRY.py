from pymongo import MongoClient
from bson import json_util
import json
from datetime import datetime
import pytz, time
import cfscrape
from colorama import init
from colorama import Fore, Back, Style
from vars import quote_assets, quote_blacklist, orderbook_object, orderbook_interval
from utils import check_rate_limit, utc_ts_now_ms, store_last_api_call_ts, ts_strf, fmt_paribu_symbol

init()
symbol = "btc-tl"
n = 5  # marketmatches max 20
derinlik_degeri = 50_000  # al veya sat tahtasında ilk x lira değerindeki teklifleri al
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

def get_depth_sum(rec, target = derinlik_degeri):
    qty_sum = 0
    value = 0
    average_price = 0
    depth = 0

    # "amount" key used for marketMatches, "qty" key used for orderbook
    qty_key = "amount" if "amount" in rec[0] else "qty"

    for i in range(len(rec)):
        value += rec[i]['price'] * rec[i][qty_key]
        qty_sum += rec[i][qty_key]
        depth = i
        if value >= target:
            average_price = value / qty_sum
            break
    else:
        average_price = value / qty_sum

    return qty_sum, value, average_price, depth + 1

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
        
        sells_qty_sum, sells_value, sells_average_price, sells_depth = get_depth_sum(record['sells'])
        buys_qty_sum, buys_value, buys_average_price, buys_depth = get_depth_sum(record['buys'])
        matches_qty_sum, matches_value, matches_average_price, matches_depth = get_depth_sum(record['matches'], 100_000)

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
        # usdt price
        usdt_last_doc = db['USDTTRY'].find().sort("_id", -1).limit(1)
        usdt_last_timestamp = usdt_last_doc[0]['_id']
        latency = (record['_id'] - usdt_last_timestamp)
        usdt_buys_price = usdt_last_doc[0]['buys_average_price']
        usdt_sells_price = usdt_last_doc[0]['sells_average_price']

        print(f"{dFormatted} Inserted_id: {result.inserted_id}, {record['symbol']}, avg. sell: {record['sells_average_price']} (qty: {sells_qty_sum:0.5f}, value: {sells_value:0.2f}, n={sells_depth}), avg buy: {record['buys_average_price']} (qty: {buys_qty_sum:0.5f}, value: {buys_value:0.2f}, n={buys_depth}), matches avg price: {matches_average_price:0.02f} (qty: {matches_qty_sum:0.05f}, value: {matches_value:0.2f}, n={matches_depth}) done in {round(time.time() - start, 2)} seconds")
        print(f"{dFormatted} USDTTRY avg. sell: {usdt_sells_price}, avg buy: {usdt_buys_price}, BTCTRY -> BTCUSDT: sell: {record['sells_average_price'] / usdt_sells_price:0.1f}, buy: {record['buys_average_price'] / usdt_buys_price:0.1f}, latency: {latency:0.2f} seconds")
        client.close()
        time.sleep(0.9)

    time.sleep(0.9)
