import os
import logging
import requests

from Data.source.base.DataGetter import DataGetter
from backtest.utilities.utils import get_datetime_from_ms
from trading.utilities.utils import FORMAT_YYYY_MM_DD


class FMP(DataGetter):
    def __init__(self, inst_type: str) -> None:
        super().__init__(inst_type)
        self.BASE_URL = "https://financialmodelingprep.com/api/v3/"

    def get_all_tradable_symbols(self):
        url = f"{self.BASE_URL}/available-traded/list?apikey={os.environ['FMP_API']}"
        resp = requests.get(url, timeout=30)
        if resp.ok:
            list_symbols = list(map(lambda x: x["symbol"], resp.json()))
            return list_symbols

    def get_tradable_symbols_exchange(self, exchange):
        url = f"{self.BASE_URL}/stock-screener?exchange={exchange}&apikey={os.environ['FMP_API']}"
        resp = requests.get(url, timeout=30)
        if resp.ok:
            list_symbols = list(map(lambda x: x["symbol"], resp.json()))
            return list_symbols

    def parseFmpScreenerRes(self, res_json: list):
        final_stocks = []
        for ticker in res_json:
            # market cap > 50m and from US
            if ticker["marketCap"] > 50000000 and ticker["country"] == "US":
                final_stocks.append(ticker["symbol"])
        return final_stocks

    def getUSScreenedStocks(self, **kwargs):
        # getUSScreenedStocks(marketCapMin=5000000000, exchange="nasdaq", volumeMoreThan=500000)
        base_url = self.BASE_URL + "stock-screener?country=US"
        for key, val in kwargs.items():
            base_url += f"&{key}={val}"
        final_url = base_url + f"&apikey={os.environ['FMP_API']}"
        logging.info(final_url)
        res = requests.get(final_url, timeout=30)
        if res.ok:
            return res.json()

    def get_ohlc(self, symbol, multiplier, freq, from_ms, to_ms):
        ts = freq if freq == "day" else f"{multiplier}{freq.replace('ute', '')}"
        from_datestr = get_datetime_from_ms(from_ms).strftime(FORMAT_YYYY_MM_DD)
        to_datestr = get_datetime_from_ms(to_ms).strftime(FORMAT_YYYY_MM_DD) 
        url = f"{self.BASE_URL}historical-chart/{ts}/{symbol}?from={from_datestr}&to={to_datestr}&apikey={os.environ['FMP_API']}"
        print(url)
        res = requests.get(url, timeout=30)
        if res.ok:
            return res.json()


def get_source_instance(inst_type):
    return FMP(inst_type)


# from backtest.utilities.utils import load_credentials, parse_args
# args = parse_args()
# load_credentials(args.credentials)

# sectors = [
#     "Consumer Cyclical", "Energy", "Technology", "Industrials", "Financial Services",
#     "Basic Materials", "Communication Services", "Consumer Defensive", "Healthcare", "Real Estate",
#     "Utilities", "Industrial Goods", "Financial", "Services", "Conglomerates"
# ]
# industries = [
#     "Autos", "Banks", "Banks Diversified", "Software",
#     "Banks Regional", "Beverages Alcoholic", "Beverages Brewers", "Beverages Non-Alcoholic"]
