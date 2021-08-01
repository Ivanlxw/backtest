import requests
import pandas as pd
import os
import time
import logging
from datetime import datetime
from pathlib import Path

from backtest.utilities.utils import log_message, parse_args, load_credentials

args = parse_args()
load_credentials(args.credentials)
with open(f"{os.path.dirname(__file__)}/snp500.txt", "r") as fin:
    stock_list = fin.readlines()
    stock_list = [stock.replace("\n", "") for stock in stock_list]

ABSOLUTE_FILEDIR = Path(os.path.dirname(os.path.abspath(__file__)))
logging.basicConfig(filename=ABSOLUTE_FILEDIR /
                    'logging/GetCsv.log', level=logging.INFO)


def get_av_csv(symbol, key, full=False, interval=None,):
    print(f"Getting symbol: {symbol}")
    if interval != None:
        if interval not in ['1min', '5min', '15min', '30min', '60min']:
            raise Exception(
                "Interval has to be one of the following options (string):\n'1min', '5min', '15min', '30min', '60min'")

        if full:
            url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=" + \
                symbol + "&interval=" + interval + "&outputsize=full" + "&apikey=" + key
        else:
            url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=" + \
                symbol + "&interval=" + interval + "&apikey=" + key

        if not os.path.exists(f"{interval}"):
            os.mkdir(f"{interval}")
    else:
        if full:
            url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=" + \
                symbol + "&outputsize=full" + "&apikey=" + key
        else:
            url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=" + \
                symbol + "&apikey=" + key

    res = requests.get(url)
    if res.ok:
        try:
            parsed_data = res.json()
            df = pd.DataFrame.from_dict(
                parsed_data['Time Series (Daily)'], orient='index')
            # reverse from start to end instead of end to start
            df.index = df.index.map(lambda x: pd.Timestamp(
                pd.to_datetime(x), unit="ms").normalize().value // 10**6)
            df = df.iloc[::-1]
            df.columns = ["open", "high", "low", "close", "volume"]
            merge_n_save(symbol, df.sort_index())
        except Exception as e:
            print(e)
            raise Exception(parsed_data)
    else:
        print(res.content)


def get_tiingo_eod(ticker, full: bool, key):
    headers = {
        'Content-Type': 'application/json'
    }
    if full:
        start_date = "2000-1-1"
        end_date = datetime.today().strftime('%Y-%m-%d')
    else:
        start_date = "2020-1-1"
        end_date = datetime.today().strftime('%Y-%m-%d')

    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate={start_date}&endDate={end_date}&resampleFreq=daily&token={key}"

    requestResponse = requests.get(url, headers=headers)
    if requestResponse.ok:
        try:
            json_df = requestResponse.json()
            df = pd.DataFrame(json_df)
            df['date'] = df['date'].apply(
                lambda x: x.replace("T00:00:00.000Z", ""))
            df.index = df['date']
            df = df.drop("date", axis=1)
            df.index = df.index.map(lambda x: pd.Timestamp(
                pd.to_datetime(x), unit="ms").normalize().value // 10**6)
            df = df[["open", "high", "low", "close", "volume"]]
            merge_n_save(ticker, df.sort_index())
        except Exception as e:
            print(requestResponse.json())
            print(e)


def merge_n_save(ticker, df):
    daily_fp = os.path.join(os.path.abspath(
        os.path.dirname(__file__)), 'data', 'daily')
    filepath = os.path.join(daily_fp, ticker+".csv")
    if not os.path.exists(daily_fp):
        os.makedirs(daily_fp)
    if os.path.exists(os.path.join(daily_fp, ticker)):
        # merge data
        existing_df = pd.read_csv(filepath, index_col=0)
        df = pd.concat([existing_df, df])
        df = df[~df.index.duplicated(keep='last')]
        df.sort_index(inplace=True)
        assert df.index.nunique() == df.index.size
    df.to_csv(filepath)
    print("Data is stored at {}".format(filepath))


def refresh_data_tiingo(tiingo_key):
    for idx, ticker in enumerate(stock_list[-500:]):
        print(idx)
        get_tiingo_eod(ticker, full=True, key=tiingo_key)


def refresh_data_av(av_key):
    for idx, ticker in enumerate(stock_list[480:]):
        print(idx+1)
        if (idx+1) % 4 == 0:
            time.sleep(61)
        get_av_csv(ticker, full=True, key=av_key)


def get_price_history_tda(ticker, period_type: str, period: int, frequency_type: str, frequency: int):
    assert frequency_type in ["minute", "daily", "weekly", "monthly"]
    assert frequency in [1, 5, 10, 15, 30]
    try:
        res = requests.get(
            f"https://api.tdameritrade.com/v1/marketdata/{ticker}/pricehistory",
            params={
                "apikey": os.environ["TDD_consumer_key"],
                "periodType": period_type,
                "period": period,
                "frequencyType": frequency_type,
                "frequency": frequency,
            },
        )
    except requests.exceptions.RequestException as e:
        log_message(f"Cannot get {ticker}")
        return None
    if res.ok:
        return res.json()
    return None


def get_tda_price_hist(sym_list):
    dne = []
    for idx, sym in enumerate(sym_list):
        log_message(idx+1)
        df = get_price_history_tda(
            sym, period_type="year", period=20, frequency=1, frequency_type="daily")
        if df is not None and "candles" in df.keys() and not df["empty"]:
            df = pd.DataFrame(df["candles"])
            df.set_index("datetime", inplace=True)
            df.index = df.index.map(lambda x: pd.Timestamp(
                x, unit="ms").normalize().value // 10 ** 6)
            merge_n_save(sym, df)
        else:
            dne.append(sym)
    log_message(f"Failed symbols: f{dne}")


if __name__ == "__main__":
    get_av_csv("SPY", full=True, key=os.environ["alpha_vantage_key"])
    # get_tiingo_eod("GTT", full=True, key=os.environ["TIINGO_API"])
    # refresh_data_av(os.environ["alpha_vantage_key"])

    # from multiprocessing import Process
    # p1 = Process(target=get_tda_price_hist, args=(['JKHY', 'LDOS'],))
    # p2 = Process(target=get_tda_price_hist, args=(stock_list[700:],))
    # p1.start()
    # p2.start()
    # p1.join()
    # p2.join()
