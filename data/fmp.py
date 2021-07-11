import os
import json

from backtest.utilities.utils import load_credentials, parse_args, remove_bs
import requests

BASE_URL = "https://financialmodelingprep.com/api/v3/"
with open(f"{os.path.dirname(__file__)}/dow_stock_list.txt", "r") as fin:
    stock_list = fin.readlines()
dow_stock_list = list(map(remove_bs, stock_list))
with open(f"{os.path.dirname(__file__)}/snp500.txt", "r") as fin:
    stock_list = fin.readlines()
snp500 = list(map(remove_bs, stock_list))


def get_all_tradable_symbols():
    url = f"https://financialmodelingprep.com/api/v3/available-traded/list?apikey={os.environ["FMP_API"]}"
    resp = requests.get(url)
    if resp.ok:
        list_symbols = list(map(lambda x: x["symbol"], resp.json()))
        return list_symbols


def get_tradable_symbols_exchange(exchange):
    url = f"https://financialmodelingprep.com/api/v3/stock-screener?exchange={exchange}&apikey={os.environ["FMP_API"]}"
    resp = requests.get(url)
    if resp.ok:
        list_symbols = list(map(lambda x: x["symbol"], resp.json()))
        return list_symbols


def parseFmpScreenerRes(res_json: list):
    final_stocks = []
    for ticker in res_json:
        # market cap > 50m and from US
        if ticker['marketCap'] > 50000000 and ticker['country'] == "US":
            final_stocks.append(ticker['symbol'])
    return final_stocks


def getindustryByStock():
    industry_by_stock = {}
    for stock in snp500[200:400]:
        res = requests.get(
            f"https://financialmodelingprep.com/api/v3/profile/{stock}?apikey={os.environ['FMP_API']}")
        if res.ok:
            # print(res.json())
            try:
                res = res.json()[0]
                if res["industry"] in industry_by_stock.keys():
                    industry_by_stock[res["industry"]].add(stock)
                else:
                    industry_by_stock[res["industry"]] = {stock}  # set
            except Exception as e:
                print(res.json())
                print(e)

    # convert dict of set to dict of list
    for k in industry_by_stock.keys():
        industry_by_stock[k] = list(industry_by_stock[k])

    curr_data = {}
    with open("industry_by_symbols.json", "r+") as f:
        curr_data = json.load(f)

    with open("industry_by_symbols.json", "w") as f:
        # combine curr_data and industry_by_stock
        for k in curr_data.keys():
            if k in industry_by_stock.keys():
                industry_by_stock[k] += curr_data[k]
            else:
                industry_by_stock[k] = curr_data[k]
        json.dump(industry_by_stock, f)


def getScreenedStocks(marketCapMin=None):
    base_url = BASE_URL + "stock-screener?country=US"
    if marketCapMin is not None:
        base_url += f"&marketCapMoreThan={marketCapMin}"
    final_url = base_url + f"&apikey={os.environ['FMP_API']}"
    res = requests.get(final_url)
    if res.ok:
        return res.json()


args = parse_args()
load_credentials(args.credentials)

sectors = [
    "Consumer Cyclical", "Energy", "Technology", "Industrials", "Financial Services",
    "Basic Materials", "Communication Services", "Consumer Defensive", "Healthcare", "Real Estate",
    "Utilities", "Industrial Goods", "Financial", "Services", "Conglomerates"
]
industries = [
    "Autos", "Banks", "Banks Diversified", "Software",
    "Banks Regional", "Beverages Alcoholic", "Beverages Brewers", "Beverages Non-Alcoholic"]

stockList = getScreenedStocks(marketCapMin=50000000)
stockList = [stock for stock in stockList if stock['isActivelyTrading'] == True]
stockList = list(map(lambda x: x["symbol"], stockList))
with open(f"{os.path.dirname(__file__)}/stock_universe.txt", "w") as f:
    f.write("\n".join(stockList))
