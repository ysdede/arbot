from pymongo import MongoClient
from datetime import datetime
import pytz, time
from vars import paribu_rate_limit

# client = MongoClient()
# db_rate_limit = client.rate_limits

def store_last_api_call_ts(exchange, timestamp, worker: str = "Unknown"):
    client = MongoClient()
    db_rate_limit = client.rate_limits
    try:
        res = db_rate_limit[exchange].insert_one({"timestamp": timestamp, 'worker': worker})
        # print(f"{exchange} rate limit stored. {res.inserted_id}")
    except Exception as e:
        print(f"{exchange} store rate limit error.\n {e}")
    
    client.close()

def get_last_api_call_ts(exchange):
    client = MongoClient()
    db_rate_limit = client.rate_limits
    try:
        last_call = db_rate_limit[exchange].find().sort("timestamp", -1).limit(1)
        # lastDoc = db_rate_limit[exchange].find().sort([('_id', -1)]).limit(1)
        # print('last call:', last_call[0])
        return last_call[0]['timestamp'], last_call[0]['worker']
    except Exception as e:
        print(f"{exchange} get rate limit error.\n {e}")
    
    client.close()

def check_rate_limit(exchange, interval: int = paribu_rate_limit):
    last_call, worker = get_last_api_call_ts(exchange)
    if last_call:
        if utc_ts_now_ms() - last_call < interval:
            print(f"{exchange} rate limit error. Last call: {ts_strf(last_call)} by {worker}")
            return False
    return True

def utc_ts_now_ms():
    return int(datetime.now(tz=pytz.utc).timestamp() * 1000)

def ts_strf(ts):
    return datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")

def fmt_paribu_symbol(symbol):
    symbol = symbol.replace("_TL", "TRY")
    symbol = symbol.replace("_USDT", "USDT")
    symbol = symbol.replace("-tl", "TRY")
    return symbol.upper()
