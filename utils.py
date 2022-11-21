import random
from pymongo import MongoClient
from datetime import datetime
import pytz, time
from vars import paribu_rate_limit
import requests
from vars import trade_rule_urls, quote_assets, quote_blacklist

# client = MongoClient()
# db_rate_limit = client.rate_limits

def get_collections(db_name: str) -> list:
    client = MongoClient()
    db = client[db_name]
    collections = db.list_collection_names()
    client.close()
    return collections

def store_last_api_call_ts(exchange, timestamp, worker: str = "Unknown"):
    client = MongoClient()
    db_rate_limit = client.rate_limits
    try:
        res = db_rate_limit[exchange].insert_one({"_id": timestamp, "timestamp": datetime.fromtimestamp(timestamp / 1000, tz=pytz.utc) , 'worker': worker})
        # print(f"{exchange} rate limit stored. {res.inserted_id}")
    except Exception as e:
        # print(f"{exchange} store rate limit error.\n {e}")
        # if "duplicate key error" in str(e):
        #     store_last_api_call_ts(exchange, timestamp + 1, worker)
        #     time.sleep(0.003)
        # print(datetime.fromtimestamp(timestamp / 1000, tz=pytz.utc))
        other_worker = db_rate_limit[exchange].find_one({"_id": timestamp})['worker']
        print(f"{exchange} last call already saved by {other_worker}.")
    
    client.close()

def get_last_api_call_ts(exchange):
    client = MongoClient()
    db_rate_limit = client.rate_limits
    try:
        last_call = db_rate_limit[exchange].find().sort("_id", -1).limit(1)
        # lastDoc = db_rate_limit[exchange].find().sort([('_id', -1)]).limit(1)
        # print('last call:', last_call[0])
        return last_call[0]['_id'], last_call[0]['worker']
    except Exception as e:
        print(f"{exchange} get rate limit error.\n {e}")
        if "no such item for Cursor instance" in str(e):
            return 1650000000000, "Unknown"
    
    client.close()

def check_rate_limit(exchange, interval: int = paribu_rate_limit):
    last_call, worker = get_last_api_call_ts(exchange)
    if last_call:
        if utc_ts_now_ms() - last_call < interval:
            print(f"{exchange} Soft rate limit. Last call: {ts_strf(last_call)} by {worker}")
            return False
    return True

def utc_ts_now_ms():
    return int(datetime.now(tz=pytz.utc).timestamp() * 1000)

def ts_strf(ts):
    return datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")

def dt_strf(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")

def fmt_paribu_symbol(symbol):
    symbol = symbol.replace("_TL", "TRY")
    symbol = symbol.replace("_USDT", "USDT")
    symbol = symbol.replace("-tl", "TRY")
    return symbol.upper()

def download_rules(exchange: str, local_fn: str = None):
    exc = exchange.lower()

    print(
        f"Downloading rules for {exchange}. {local_fn=}, URL: {trade_rule_urls[exc]}"
    )
    # try:
    data = requests.get(trade_rule_urls[exc]).json()

    if "serverTime" not in data.keys():
        print("if 'serverTime' not in data.keys():")
        data["serverTime"] = datetime.datetime.now().timestamp() * 1000

    # Bybit api does not return server time so we need to add it manually using our server time
    if "ret_msg" in data and data["ret_msg"] == "OK":
        data["serverTime"] = datetime.datetime.now().timestamp() * 1000
        print("Added local timestamp to Bybit data")

    if int(data["serverTime"]):
        return data

def get_binance_symbols(exchange: str):
    symbols = download_rules(exchange)["symbols"]
    
    allowed_symbols = []

    for s in symbols:
        if s["status"] != "TRADING" or not s["isSpotTradingAllowed"]:
            print(f'Skipping {s["symbol"]} s["status"] != "TRADING" or not s["isSpotTradingAllowed"]')
            continue

        if s["quoteAsset"] not in quote_assets[exchange] or s["quoteAsset"] in quote_blacklist[exchange]:
            print(f'Skipping {s["symbol"]} s["quoteAsset"] not in quote_assets[exchange] or s["quoteAsset"] in quote_blacklist[exchange]')
            continue

        allowed_symbols.append((s["symbol"], s["baseAsset"], s["quoteAsset"]))
    
    return allowed_symbols

def random_sleep():
    # sleep random time to avoid conflict with other threads
    ran_sleep = round(random.random() / 100, 6)
    # print(f"{sym} Sleeping for {ran_sleep} seconds --------------")
    time.sleep(ran_sleep)
