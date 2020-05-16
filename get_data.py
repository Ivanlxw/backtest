from backtest import utils
from data.get_csv import getData_csv, get_tiingo_eod

with open("data/stock_list.txt", 'r') as fin:
    stock_list = fin.readlines()

stock_list = list(map(utils.remove_bs, stock_list))

for idx, symbol in enumerate(stock_list):
    import time
    # if idx % 5 == 0 and idx > 0:
    #     time.sleep(65)
    # getData_csv(symbol, csv_dir="./data/daily", full=True)
    get_tiingo_eod(symbol, f"data/data/daily/{symbol}.csv", full=True)