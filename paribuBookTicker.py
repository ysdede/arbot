from pymongo import MongoClient
from bson import json_util
from datetime import datetime
import pytz, time
import cfscrape
from colorama import init
from colorama import Fore, Back, Style

init()

exchange = "Paribu"
bookTickerURL = "https://www.paribu.com/ticker"
scraper = cfscrape.create_scraper()
client = MongoClient()
db = client.paribu_bookTicker
data_json = ""
interval = 30

shortCodes = ["BTC_TL"]

d = datetime.now(tz=pytz.utc)
dFormatted = Fore.GREEN + d.strftime("%d/%m/%Y %H:%M:%S") + Style.RESET_ALL
print(f"{dFormatted} {exchange} bookTicker started...")

while True:
    d = datetime.now(tz=pytz.utc)
    ts = time.time()
    valid = "true"
    if not d.second % interval:
        print(f'{d.strftime("%d/%m/%Y %H:%M:%S")} Interval')
        try:
            response = scraper.get(bookTickerURL).content.decode("utf-8")
            # response = requests.get(bookTickerURL).text
            # response = response.replace('highestBid', 'bidPrice')
            # response = response.replace('lowestAsk', 'askPrice')
        except Exception as e:
            print(f"{exchange} api hatası.\n {e}")

        oldData_json = data_json
        data_json = json_util.loads(response)
        # print(response, type(response))
        print(data_json, type(data_json))

        if "highestBid" not in response:
            print("Hata, önceki veriler kaydediliyor...")
            data_json = oldData_json
            valid = "false"

        # for code in shortCodes:
        for k, v in data_json.items():
            symbol = k.replace("_TL", "TRY")
            print(data_json[k])
            bidPrice = v["highestBid"]
            askPrice = v["lowestAsk"]
            v["timestamp"] = d
            v["_id"] = int(ts)
            v["valid"] = valid
            v["symbol"] = symbol
            print(f"------------------\n{v}\n------------------")
            result = db[symbol].insert_one(v)
            dFormatted = Fore.GREEN + d.strftime("%d/%m/%Y %H:%M:%S") + Style.RESET_ALL
            print(f"{dFormatted} Inserted_id: {result.inserted_id}, {symbol}, {askPrice}, {bidPrice}")


        print(f"{exchange} bookTicker done...")
        client.close()
        time.sleep(0.9)

    time.sleep(0.9)
