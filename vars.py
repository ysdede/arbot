hedef_derinlik = 50_000

ticker_interval = 20
orderbook_interval = 30

paribu_rate_limit = 1500  # ms
binance_rate_limit = 25  # ms

rate_limits = {
    "paribu": paribu_rate_limit,
    "binance": binance_rate_limit,
}

paribu_interval_codes = ["6h"]

orderbook_urls = {
    "binance": "https://api.binance.com/api/v3/depth?symbol={self.symbol}&limit={self.limit}",
    "paribu": "https://v3.paribu.com/app/markets/{symbol}?interval={interval}",
}

http_codes = {
        "200": "OK",
        "400": "Bad Request",
        "401": "Unauthorized",
        "403": "Forbidden",
        "404": "Not Found",
        "429": "Too Many Requests",
}

ticker_object = {
    "_id": None,
    "timestamp": None,
    "symbol": None,
    "baseAsset": None,
    "quoteAsset": None,
    "bidPrice": None,
    "bidQty": None,
    "askPrice": None,
    "askQty": None,
    "last": None,
    "volume": None,
    "average": None,
    "daily": None,
    "daily_perc": None,
    "valid": None,
    "change": None,  # TODO: !
}

orderbook_object = {
    "_id": None,
    "timestamp": None,
    "sells": None,
    "buys": None,
    "matches": None,
    "valid": None,
}

mappings = {
    "btcturk": {
        "symbol": "pair",
        "bidPrice": "bid",
        "askPrice": "ask",
        "volume": "volume",
        "daily": "daily",
        "daily_perc": "dailyPercent",
        "baseAsset": "numeratorSymbol",
        "quoteAsset": "denominatorSymbol",
        "last": "last",
        "average": "average",
        "bidQty": None,
        "askQty": None,
        "timestamp": "timestamp",
    },
    "binance": {
        "symbol": "symbol",
        "bidPrice": "bidPrice",
        "askPrice": "askPrice",
        "bifQty": "bidQty",
        "askQty": "askQty",
    },
    "paribu": {
        "symbol": None,
        "bidPrice": "highestBid",
        "askPrice": "lowestAsk",
        "volume": "volume",
        "last": "last",
        "average": "avg24hr",
        "bidQty": None,
        "askQty": None,
        "change": "change",
    },
}

quote_assets = {
    "btcturk": ["USDT", "TRY"],
    "binance": ["USDT", "TRY"],
    "paribu": ["USDT", "_TL", "TRY", "-tl"],
}

quote_blacklist = {
    "btcturk": ["UPUSDT", "DOWNUSDT"],
    "binance": ["UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT"],
    "paribu": ["UPUSDT", "DOWNUSDT"],
}

btcturk_ticker_Sample = {
    "pair": "BTCTRY",
    "pairNormalized": "BTC_TRY",
    "timestamp": 1668789450749,
    "last": 317724.0,
    "high": 322979.0,
    "low": 316430.0,
    "bid": 317724.0,
    "ask": 317890.0,
    "open": 317137.0,
    "volume": 136.92353223,
    "average": 318428.45,
    "daily": 753.0,
    "dailyPercent": 0.19,
    "denominatorSymbol": "TRY",
    "numeratorSymbol": "BTC",
    "order": 1000,
}

binance_ticker_sample = '{"symbol":"ETHBTC","bidPrice":"0.07258900","bidQty":"14.06430000","askPrice":"0.07259000","askQty":"0.02020000"}'

paribu_ticker_sample = {
    "BTC_TL": {
        "lowestAsk": 318100.01,
        "highestBid": 317800,
        "low24hr": 313108,
        "high24hr": 322356,
        "avg24hr": 318408.33124565,
        "volume": "379.89373187",
        "last": 318100.01,
        "change": 1767.01,
        "percentChange": 0.6,
        "chartData": [],
    }
}

trade_rule_urls = {
    "Binance": "https://api.binance.com/api/v1/exchangeInfo",
    "Binance Futures": "https://fapi.binance.com/fapi/v1/exchangeInfo",
    "Bybit Perpetual": "https://api.bybit.com/v2/public/symbols",
}
