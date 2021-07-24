"""
  A script to update data daily. Especially useful for intraday data as history is short and we should save data
  Updates using TDA API since we are eventually trading with TDA
"""
import os
import time
import logging
import requests
import pandas as pd
from pathlib import Path

from backtest.utilities.utils import load_credentials, log_message, parse_args, remove_bs

# TODO: use direnv
args = parse_args()
load_credentials(args.credentials)

ABSOLUTE_FILEDIR = Path(os.path.dirname(os.path.abspath(__file__)))
logging.basicConfig(filename=ABSOLUTE_FILEDIR /
                    'logging/DataWriter.log', level=logging.INFO)
with open(ABSOLUTE_FILEDIR / "stock_universe.txt") as fin:
    SYM_LIST = list(map(remove_bs, fin.readlines()))
NY = 'America/New_York'


def get_price_history(ticker, period_type: str, period: int, frequency_type: str, frequency: int):
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


def _get_historical_dcf(sym: str):
    url = f"https://financialmodelingprep.com/api/v3/historical-daily-discounted-cash-flow/{sym}?period=quarter&apikey={os.environ['FMP_API']}"
    resp = requests.get(url)
    if resp.ok:
        dcf = pd.DataFrame(resp.json()).iloc[::-1]
        if dcf.empty:
            raise Exception("dcf dataframe empty")
        dcf.set_index("date", inplace=True)
        dcf.index = dcf.index.map(pd.to_datetime)
        dcf.reindex(pd.date_range(
            dcf.index[0], dcf.index[-1], freq='d'), method="pad")
        return dcf


def _get_historical_financial_growth(sym: str):
    url = f"https://financialmodelingprep.com/api/v3/financial-growth/{sym}?period=quarter&apikey={os.environ['FMP_API']}"
    resp = requests.get(url)
    if resp.ok:
        financial_growth = pd.DataFrame(resp.json()).iloc[::-1]
        if financial_growth.empty:
            raise Exception("financial_growth dataframe empty")
        financial_growth.set_index("date", inplace=True)
        financial_growth.index = financial_growth.index.map(pd.to_datetime)
        return financial_growth


def get_fundamentals_hist_quarterly(ticker: str):
    dfs = []
    dfs.append(_get_historical_dcf(ticker))
    dfs.append(_get_historical_financial_growth(ticker))
    return pd.concat(dfs, axis=1, join="inner")


def update_data(freq: str):
    csv_dir = ABSOLUTE_FILEDIR / f"data/{freq}"
    if not os.path.exists(csv_dir):
        raise Exception(
            f"File for frequency ({freq}) does not exist")
    if freq == "daily":
        period_type = "month"
        period = 1
        frequency_type = "daily"
        freq = 1
    else:
        assert freq.lower().endswith("min")
        period_type = "day"
        period = 10
        frequency_type = "minute"
        freq = int(freq[:-3])

    missing_syms = []
    for symbol in SYM_LIST:
        df = get_price_history(symbol, period_type,
                               period, frequency_type, freq)  # json format
        if df is not None and "candles" in df.keys() and not df["empty"]:
            data = pd.DataFrame(df["candles"])
            data.set_index("datetime", inplace=True)
            # need to convert time of data to 12am -- only for daily
            if freq == "daily":
                data.index = data.index.map(lambda x: pd.Timestamp(
                    x, unit="ms").normalize().value // 10 ** 6)
            csv_fp = csv_dir / f"{symbol}.csv"
            if os.path.exists(csv_fp):
                existing = pd.read_csv(csv_fp, index_col=0)
                data = pd.concat(
                    [existing, data], axis=0)
                data = data[~data.index.duplicated(keep='last')]
            data.to_csv(csv_fp)
        else:
            missing_syms.append(symbol)

    # write to a txt
    with open(ABSOLUTE_FILEDIR / "missing_sym.txt", 'w') as fin:
        fin.write("\n".join(missing_syms))


def update_ohlc(time_freq: str):
    assert time_freq in ["daily", "1min", "5min", "10min", "15min", "30min"]
    update_data(time_freq)
    log_message(f"update_data done for timeframe {time_freq}")
    time.sleep(60)


def main():
    timeframes = ["daily", "1min", "5min", "10min", "15min", "30min"]
    while True:
        now = pd.Timestamp.now(tz=NY)
        if not (now.hour == 0 and now.minute == 15 and now.dayofweek > 0):
            continue
        for time_freq in timeframes:
            update_ohlc(time_freq)
        if now.dayofweek >= 5:
            break
        time.sleep(20 * 3600)  # 20 hrs


def write_fundamental():
    fundamental_dir = ABSOLUTE_FILEDIR / "data/fundamental/quarterly"
    failed_syms = []
    for sym in SYM_LIST:
        try:
            data = get_fundamentals_hist_quarterly(sym)
            csv_fp = fundamental_dir / f"{sym}.csv"
            if os.path.exists(csv_fp):
                existing = pd.read_csv(csv_fp, index_col=0, header=0)
                existing.index = existing.index.map(pd.to_datetime)
                cols_to_use = data.columns.difference(existing.columns)
                data = pd.merge(
                    existing, data[cols_to_use], left_index=True, right_index=True, how="outer")
                data = data[~data.index.duplicated(keep='last')]
            data.to_csv(csv_fp)
        except Exception as e:
            log_message(f"[write_fundamental] {sym} failed with : {e}")
            failed_syms.append(sym)
    log_message(f"[write_fundamental] Failed syms - {failed_syms}")


if __name__ == "__main__":
    # main()
    # write_fundamental()
    update_ohlc("daily")
