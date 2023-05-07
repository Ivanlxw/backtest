import argparse
import datetime
import importlib
import concurrent.futures as fut
from pathlib import Path
import time

import pandas as pd

from Data.source.base.DataGetter import DataGetter
from backtest.utilities.utils import DATA_GETTER_INST_TYPES, NY_TIMEZONE, OPTION_METADATA_PATH, get_ms_from_datetime, load_credentials, get_sleep_time, read_universe_list

DATA_FROM = datetime.datetime(2021, 6, 1)
DATA_TO = datetime.datetime(2023, 4, 30)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Get ticker csv data via API calls to either AlphaVantage or Tiingo."
    )
    parser.add_argument("-c", "--credentials", required=True,
                        type=str, help="filepath to credentials.json",)
    parser.add_argument("-s", "--source", required=True, type=str)
    parser.add_argument("--universe", type=Path,  required=True,
                        help="File path to trading universe", nargs="+")
    parser.add_argument("--inst-type", type=str, required=False,
                        default='equity', choices=DATA_GETTER_INST_TYPES)
    parser.add_argument('-l', '--live', action='store_true',
                        help='inform life?')
    return parser.parse_args()


def get_source_instance(source, universe_fp, inst_type: str):
    importlib.util.find_spec(f"Data.source.{source}")
    return importlib.import_module(f"Data.source.{source}").get_source_instance(
        universe_fp, inst_type
    )


def get_and_write_stock_symbols(getter, exchange):
    stockList = getter.getUSScreenedStocks(
        exchange=exchange, volumeMoreThan=1600000, marketCapMoreThan=50000000
    )
    stockList = list(
        map(
            lambda x: x["symbol"],
            [stock for stock in stockList if stock["isActivelyTrading"]],
        )
    )
    with open(f"./Data/data/universe/nyse_largecap_active.txt", "w") as f:
        f.write("\n".join(stockList))


def get_option_ticker_from_underlying(underlying_list):
    option_tickers = []
    with pd.HDFStore(OPTION_METADATA_PATH) as hdf:
        for underlying in underlying_list:
            underlying_key = f"/{underlying}"
            if underlying_key not in hdf.keys():
                continue
            option_info_df = hdf[underlying_key]
            option_info_df.expiration_date = pd.to_datetime(
                option_info_df.expiration_date)
            option_tickers.extend(list(option_info_df.query(
                f" {DATA_FROM.strftime('%Y%m%d')} <= expiration_date <= {DATA_TO.strftime('%Y%m%d')}").ticker.unique()))
    return option_tickers


if __name__ == "__main__":
    args = parse_args()
    assert args.inst_type == "equity" or (
        args.source == "polygon" and not args.live), "getting options ohlc does not support life nor non-polygon source."
    load_credentials(args.credentials, into_env=True)
    universe_list = read_universe_list(args.universe)
    if args.inst_type == "options":
        universe_list = get_option_ticker_from_underlying(universe_list)
    getter: DataGetter = get_source_instance(args.source, universe_list, args.inst_type)
    processes = []
    while True:
        # Update the bars (specific backtest code, as opposed to live trading)
        now = pd.Timestamp.now(tz=NY_TIMEZONE)
        time_since_midnight = now - now.normalize()
        if args.live and ((now.dayofweek == 4 and now.hour > 17) or now.dayofweek > 4):
            break
        elif args.live and (time_since_midnight < datetime.timedelta(hours=7) or time_since_midnight > datetime.timedelta(hours=17, minutes=45)):
            time.sleep(60)
            continue
        today = datetime.datetime.today()
        history = today - datetime.timedelta(days=35)
        from_ms = get_ms_from_datetime(history if args.live else DATA_FROM)
        to_ms = get_ms_from_datetime(today if args.live else DATA_TO)
        print(from_ms, to_ms)
        with fut.ThreadPoolExecutor(max_workers=8) as e:
            for sym in getter.universe_list:
                processes.append(e.submit(getter.write_ohlc,
                                            sym, 1, "day", from_ms, to_ms))
            if args.inst_type == "equity":
                for sym in getter.universe_list:
                    for multiplier in [30, 60]:  # 1, 5, 15,
                        processes.append(
                            e.submit(getter.write_ohlc, sym, multiplier, "minute", from_ms, to_ms))
            time.sleep(1)
            processes = [p.result() for p in processes if p is not None]
        if not args.live:
            break
        time.sleep(get_sleep_time("60minute"))
