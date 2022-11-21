from pymongo import MongoClient
import json
from datetime import datetime
import pytz, time
import cfscrape
from vars import (
    quote_assets,
    quote_blacklist,
    orderbook_object,
    rate_limits,
    orderbook_urls,
)
import vars
from utils import (
    check_rate_limit,
    utc_ts_now_ms,
    store_last_api_call_ts,
    dt_strf,
)

import logging
import logging.handlers

class OrderBookBase(object):
    def __init__(self, n: int = 5, limit: int = 10):
        self.symbol = None
        self.limit = limit
        self.exchange = None
        self.scraper = cfscrape.create_scraper()
        self.client = MongoClient()
        self.db = self.client[self.__class__.__name__]
        self.data_json = ""
        self.n = n
        self.d = datetime.now(tz=pytz.utc)
        self.dFormatted = dt_strf(self.d)
        self.valid = True
        print(self.__class__.__name__, "instance created.")

    def validate_json(self, data_json):
        """Binance için geçerli.
        Diğer borsalar için override edilecek.
        """
        if (
            "lastUpdateId" not in data_json
            or "bids" not in data_json
            or "asks" not in data_json
        ):
            print(
                f"Veri hatalı, ham veriyi kontrol edin. {self.orderbookURL} {data_json}"
            )
            return False
        return True

    def make_request(self):
        return self.scraper.get(self.orderbookURL).content.decode("utf-8")

    def get_orderbook(self):
        while not check_rate_limit(self.exchange, rate_limits[self.exchange]):
            time.sleep(rate_limits[self.exchange] / 1000)
        try:
            api_call_ts = utc_ts_now_ms()
            response = self.make_request()
            store_last_api_call_ts(
                self.exchange, api_call_ts, f"{self.db.name}_{self.symbol}"
            )
        except Exception as e:
            print(f"{self.exchange} api hatası.\n {e}\n {self.orderbookURL}")
            if "429" in str(
                e
            ):  # Bu kod Binance için, diğer borsalar için değiştirilebilir.
                # A Retry-After header is sent with a 418 or 429 responses and will give the number of seconds required to wait
                # before making a new request.
                print("Rate limit, 429 hatası. 5 saniye bekleniyor.")
                time.sleep(5)
                self.get_orderbook()
        self.data_json = json.loads(response)

        if not self.validate_json(self.data_json):
            return None

        res = self.parse_orderbook(self.data_json)
        res["ts"] = int(api_call_ts / 1000)
        return res

    def parse_orderbook(self, data_json):
        sells, buys, mm = [], [], []
        sells.extend(
            {"price": float(a[0]), "qty": float(a[1])} for a in data_json["asks"]
        )
        buys.extend(
            {"price": float(a[0]), "qty": float(a[1])} for a in data_json["bids"]
        )

        return {"sells": sells, "buys": buys, "mm": mm}

    def store_orderbook(self, record):
        try:
            return self.db[self.symbol].insert_one(record)
        except Exception as e:
            print(f"{self.exchange} veritabanı hatası.\n {e}")

    @property
    def orderbookURL(self):
        return f"https://api.binance.com/api/v3/depth?symbol={self.symbol}&limit={self.limit}"

    def get_depth_sum(self, rec, target=vars.hedef_derinlik):
        """Hedeflenen pozisyon miktarına ulaşmak için orderları topla."""
        if len(rec) == 0:
            return 0, 0, 0, 0

        qty_sum = 0
        value = 0
        average_price = 0
        depth = 0

        for i in range(len(rec)):
            value += rec[i]["price"] * rec[i]["qty"]
            qty_sum += rec[i]["qty"]
            depth = i
            if value >= target:
                average_price = value / qty_sum
                break
        else:
            average_price = value / qty_sum

        return qty_sum, value, average_price, depth + 1

    def init_logger(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.handler = logging.handlers.RotatingFileHandler(
            f"{self.__class__.__name__}.log",
            maxBytes=1000000,
            backupCount=5,
        )
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self.handler.setFormatter(formatter)
        self.logger.addHandler(self.handler)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def standardize_symbol(self, symbol):
        return symbol.upper()

    def run(self, symbol=None):
        if not symbol:
            return None

        self.init_logger()

        self.symbol = self.standardize_symbol(symbol)
        start = time.time()

        if not self.symbol.endswith(
            tuple(quote_assets[self.exchange])
        ) or self.symbol.endswith(tuple(quote_blacklist[self.exchange])):
            return None

        ob = self.get_orderbook()
        if not ob:
            self.logger.error(f"Orderbook alınamadı. {self.orderbookURL}")
            return None

        record = orderbook_object.copy()
        record["sells"], record["buys"], record["matches"], ts = (
            ob["sells"],
            ob["buys"],
            ob["mm"],
            ob["ts"],
        )

        if record["sells"] is None or record["buys"] is None or ts is None:
            return None

        record["_id"] = int(ts)
        record["exchange"] = self.exchange
        record["symbol"] = self.symbol
        record["timestamp"] = datetime.fromtimestamp(ts, tz=pytz.utc)
        record["valid"] = self.valid

        (
            sells_qty_sum,
            sells_value,
            sells_average_price,
            sells_depth,
        ) = self.get_depth_sum(record["sells"])
        buys_qty_sum, buys_value, buys_average_price, buys_depth = self.get_depth_sum(
            record["buys"]
        )
        (
            matches_qty_sum,
            matches_value,
            matches_average_price,
            matches_depth,
        ) = self.get_depth_sum(record["matches"], 100_000)

        record["sells_qty_sum"] = round(sells_qty_sum, 5)
        record["buys_qty_sum"] = round(buys_qty_sum, 5)
        record["sells_average_price"] = round(sells_average_price, 3)
        record["buys_average_price"] = round(buys_average_price, 3)
        record["sells_value"] = round(sells_value, 3)
        record["buys_value"] = round(buys_value, 3)
        record["sells_depth"] = sells_depth
        record["buys_depth"] = buys_depth
        record["matches_qty_sum"] = round(matches_qty_sum, 5)
        record["matches_average_price"] = round(matches_average_price, 3)
        record["matches_value"] = round(matches_value, 3)
        record["matches_depth"] = matches_depth
        record["n"] = self.n

        if res := self.store_orderbook(record):
            self.logger.info(
                f"{self.dFormatted} {record['symbol']}, _id: {res.inserted_id}, avg. sell: {record['sells_average_price']} (qty: {sells_qty_sum:0.5f}, value: {sells_value:0.2f}, n={sells_depth}), avg buy: {record['buys_average_price']} (qty: {buys_qty_sum:0.5f}, value: {buys_value:0.2f}, n={buys_depth}), matches avg price: {matches_average_price:0.02f} (qty: {matches_qty_sum:0.05f}, value: {matches_value:0.2f}, n={matches_depth}) done in {round(time.time() - start, 2)} seconds"
            )
            return res.inserted_id
        else:
            self.logger.warning(
                f"{self.dFormatted} {self.exchange} {self.symbol} kaydedilemedi."
            )

        self.client.close()
