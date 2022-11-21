from orderBookBase import OrderBookBase

class BinanceOrderbook(OrderBookBase):
    def __init__(self, n: int = 5, limit: int = 10):
        super().__init__(n, limit)
        self.exchange = "binance"


if __name__ == "__main__":
    binance = BinanceOrderbook()
    binance.run(symbol="BTCUSDT")
