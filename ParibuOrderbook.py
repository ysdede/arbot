from orderBookBase import OrderBookBase
from vars import (
    paribu_interval_codes,
    paribu_interval_codes,
)


class ParibuOrderbook(OrderBookBase):
    def __init__(self, n: int = 5, limit: int = 10):
        super().__init__(n, limit)
        self.exchange = "paribu"

    @property
    def paribu_url_symbol(self):
        if self.symbol.endswith("TRY"):
            return f"{self.symbol[:-3]}-tl".lower()
        elif self.symbol.endswith("USDT"):
            return f"{self.symbol[:-4]}-usdt".lower()
        else:
            raise NotImplementedError

    @property
    def orderbookURL(self):
        return f"https://v3.paribu.com/app/markets/{self.paribu_url_symbol}?interval={paribu_interval_codes[0]}"

    def validate_json(self, data_json):
        if "success" not in data_json or data_json["success"] == False:
            self.logger.error(f"Veri hatalı, ham veriyi kontrol edin: {data_json}")
            return False

        print(f"Succes: {data_json['success']}")
        return True

    def make_request(self):
        return self.scraper.get(self.orderbookURL).content.decode("utf-8")

    def parse_orderbook(self, data_json):
        sells, buys, mm = [], [], []

        for k, v in data_json["data"]["orderBook"]["sell"].items():
            sells.append({"price": float(k), "qty": float(v)})

        for k, v in data_json["data"]["orderBook"]["buy"].items():
            buys.append({"price": float(k), "qty": float(v)})

        for m in data_json["data"]["marketMatches"]:
            mm.append({"price": float(m["price"]), "qty": float(m["amount"])})

        return {"sells": sells, "buys": buys, "mm": mm}


if __name__ == "__main__":
    paribu = ParibuOrderbook()
    paribu.run(symbol="BTCTRY")
