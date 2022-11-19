from pymongo import MongoClient
import json
from datetime import datetime
import pytz, time
import cfscrape
from colorama import init
from colorama import Fore, Back, Style
from vars import quote_assets, quote_blacklist, orderbook_object, binance_rate_limit
from utils import check_rate_limit, utc_ts_now_ms, store_last_api_call_ts, ts_strf, dt_strf

init()
class BinanceObClass:
    def __init__(self, symbol, limit: int = 10, n: int = 5):
        self.symbol = symbol
        self.limit = limit
        self.exchange = 'binance'
        self.orderbookURL = f'https://api.binance.com/api/v3/depth?symbol={symbol}&limit={limit}'
        self.scraper = cfscrape.create_scraper()
        self.client = MongoClient()
        self.db = self.client.binance_orderbook
        self.data_json = ''
        self.n = n
        self.d = datetime.now(tz=pytz.utc)
        self.dFormatted = Fore.GREEN + dt_strf(self.d) + Style.RESET_ALL
        self.valid = True
        # print(f'{self.dFormatted} {self.exchange} orderbook started...')

    def get_orderbook(self):
        while not check_rate_limit(self.exchange, binance_rate_limit):
            time.sleep(0.01)
        try:
            api_call_ts = utc_ts_now_ms()
            response = self.scraper.get(self.orderbookURL).content.decode('utf-8')
            store_last_api_call_ts(self.exchange, api_call_ts, self.db.name)
            # print(response, type(response))
        except Exception as e:
            print(f'{self.exchange} api hatası.\n {e}\n {self.orderbookURL}')
        self.data_json = json.loads(response)
        if 'lastUpdateId' not in response or 'bids' not in response or 'asks' not in response:
            print(f'Veri hatalı, ham veriyi kontrol edin. {self.orderbookURL} {response}')
            return None, None, None
        return *self.parse_orderbook(self.data_json), int(api_call_ts / 1000)

    def parse_orderbook(self, data_json):
        sells, buys = [], []
        for a in data_json['asks']:
            sells.append({'price': float(a[0]), 'qty': float(a[1])})
        for a in data_json['bids']:
            buys.append({'price': float(a[0]), 'qty': float(a[1])})
        return sells, buys

    def store_orderbook(self, record):
        try:
            # return self.db.insert_one(record)
            return self.db[self.symbol].insert_one(record)
        except Exception as e:
            print(f'{self.exchange} veritabanı hatası.\n {e}')

    def run(self):
        start = time.time()

        if not self.symbol.endswith(tuple(quote_assets[self.exchange])) or self.symbol.endswith(tuple(quote_blacklist[self.exchange])):
            return None

        sells, buys, ts = self.get_orderbook()

        if sells is None or buys is None or ts is None:
            return None

        record = orderbook_object.copy()

        record['_id'] = int(ts)
        record['exchange'] = self.exchange
        record['symbol'] = self.symbol
        record["timestamp"] = datetime.fromtimestamp(ts, tz=pytz.utc)
        record['sells'] = sells
        record['buys'] = buys
        record['matches'] = None
        record['valid'] = self.valid    
            
        sells_qty_sum = 0
        buys_qty_sum = 0
        sells_avg_price = 0
        buys_avg_price = 0
        sells_value = 0
        buys_value = 0
        matches_qty_sum = 0
        matches_value = 0
        matches_avg_price = 0

        for i in range(0, self.n):
            # print(i, record['sells'][i]['qty'], record['sells'][i]['price'])
            sells_value += record['sells'][i]['qty'] * record['sells'][i]['price']
            # print(f"{record['sells'][i]['qty']=} * {record['sells'][i]['price']=} = {sells_value=}")
            buys_value += record['buys'][i]['qty'] * record['buys'][i]['price']
            # matches_value += record['matches'][i]['amount'] * record['matches'][i]['price']

            sells_qty_sum += record['sells'][i]['qty']
            buys_qty_sum += record['buys'][i]['qty']
            # matches_qty_sum += record['matches'][i]['amount']

            sells_average_price = sells_value / sells_qty_sum
            buys_average_price = buys_value / buys_qty_sum
            # matches_average_price = matches_value / matches_qty_sum
            matches_average_price = 0
        
        record['sells_qty_sum'] = round(sells_qty_sum, 5)
        record['buys_qty_sum'] = round(buys_qty_sum, 5)
        record['sells_average_price'] = round(sells_average_price, 3)
        record['buys_average_price'] = round(buys_average_price, 3)
        record['sells_value'] = round(sells_value, 3)
        record['buys_value'] = round(buys_value, 3)
        # record['matches_qty_sum'] = round(matches_qty_sum, 5)
        # record['matches_value'] = round(matches_value, 3)
        # record['matches_average_price'] = round(matches_average_price, 3)
        record['n'] = self.n

        res = self.store_orderbook(record)
        if res is not None:
            print(f"{self.dFormatted} {self.exchange} {self.symbol} {res.inserted_id}, avg. sell: {record['sells_average_price']} (qty: {sells_qty_sum:0.3f}, Total: {sells_value:0.2f}), avg buy: {record['buys_average_price']} (qty: {buys_qty_sum:0.3f}, Total: {buys_value:0.2f}), matches avg price: {matches_average_price:0.03f} (qty: {matches_qty_sum:0.0f}) done in {round(time.time() - start, 2)} seconds")
            return res.inserted_id
        else:
            print(f'{self.dFormatted} {self.exchange} {self.symbol} kaydedilemedi.')
        
        self.client.close()

if __name__ == '__main__':
    symbol = 'BTCUSDT'
    orderbook = BinanceObClass(symbol)
    orderbook.run()
