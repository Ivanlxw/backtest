import argparse
import json
import time
import logging
from trading''.utilities.utils import remove_bs
from data.get_csv import get_av_csv, get_tiingo_eod


def parse_args():
    parser = argparse.ArgumentParser(
        description='Get ticker csv data via API calls to either AlphaVantage or Tiingo.')

    parser.add_argument('-c', '--credentials', required=True,
                        type=str, help="filepath to credentials.json")
    parser.add_argument('--alphavantage', required=False,
                        type=str, help="use alphavantage instead of tiingo")
    return parser.parse_args()


args = parse_args()
with open(args.credentials, 'r') as fin:
    KEY = json.load(fin)["APIKEY"]

with open("data/stock_list.txt", 'r') as fin:
    stock_list = fin.readlines()

stock_list = list(map(remove_bs, stock_list))

for idx, symbol in enumerate(stock_list):
    try:
        if args.alphavantage:
            logging.info("Using alphavantage")
            if idx % 5 == 0 and idx > 0:
                time.sleep(65)
            get_av_csv(symbol, csv_dir="./data/data/daily", full=True, key=KEY)
        else:
            logging.info("Using tiingo")
            get_tiingo_eod(
                symbol, f"data/data/daily/{symbol}.csv", full=True, key=KEY)
    except Exception as e:
        raise KeyError(f"Symbol: {symbol}, error: {e}")
