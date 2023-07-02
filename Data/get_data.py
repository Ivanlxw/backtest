import argparse
import datetime
import importlib
from multiprocessing import Pool
import os
from pathlib import Path
import subprocess
import time

import pandas as pd

from Data.source.base.DataGetter import DataGetter
from backtest.utilities.option_info import get_option_ticker_from_underlying
from backtest.utilities.utils import (
    DATA_GETTER_INST_TYPES,
    NY_TIMEZONE,
    get_ms_from_datetime,
    load_credentials,
    get_sleep_time,
    read_universe_list,
)

# change this from intended start date: datetime.datetime(2022, 9, 1)
DATA_FROM = datetime.datetime(2021, 7, 1)
DATA_TO = datetime.datetime(2023, 6, 10)
DERIVE_START_MS_FROM_FILE = False
WRITE_NEW_SYMBOLS_ONLY = True


def parse_args():
    parser = argparse.ArgumentParser(description="Get ticker csv data via API calls to either AlphaVantage or Tiingo.")
    parser.add_argument(
        "-c",
        "--credentials",
        required=True,
        type=str,
        help="filepath to credentials.json",
    )
    parser.add_argument("-s", "--source", required=True, type=str)
    parser.add_argument("--universe", type=Path, required=False, help="File path to trading universe", nargs="+")
    parser.add_argument("--symbol", required=False, help="File path to trading universe", nargs="+")
    parser.add_argument("--inst-type", type=str, required=False, default="equity", choices=DATA_GETTER_INST_TYPES)
    parser.add_argument("--frequency", type=str, default="day", help="Frequency of data. Searches a dir with same name")
    parser.add_argument("-l", "--live", action="store_true", help="inform life?")
    return parser.parse_args()


def get_source_instance(source, inst_type: str):
    importlib.util.find_spec(f"Data.source.{source}")
    return importlib.import_module(f"Data.source.{source}").get_source_instance(inst_type)


def get_and_write_stock_symbols(getter, exchange):
    stockList = getter.getUSScreenedStocks(exchange=exchange, volumeMoreThan=1600000, marketCapMoreThan=50000000)
    stockList = list(
        map(
            lambda x: x["symbol"],
            [stock for stock in stockList if stock["isActivelyTrading"]],
        )
    )
    with open(f"./Data/data/universe/nyse_largecap_active.txt", "w") as f:
        f.write("\n".join(stockList))


def write_ohlc(getter: DataGetter, symbol, multiplier, freq, from_ms, to_ms):
    """min(len(universe_list), 25)
    from_ms: None if want to infer from existing dataset
    """
    fp = getter.get_fp("" if freq == "day" else multiplier, freq, symbol)
    if fp.exists() and from_ms is None:
        existing_df = pd.read_csv(fp)
        from_ms = existing_df.sort_values("t")["t"].iloc[-1]
    if from_ms is None:
        from_ms = get_ms_from_datetime(DATA_FROM)
    getter.write_ohlc(symbol, multiplier, freq, from_ms, to_ms, fp)


def _store_option_data_into_history():
    proj_root_dir = os.environ["WORKSPACE_ROOT"]
    cmd = (
        f"python {proj_root_dir}/Data/consolidate_option_data.py --credentials {proj_root_dir}/real_credentials.json "
        f"--universe {' '.join([str(univ_path) for univ_path in args.universe])}"
    )
    subprocess.run(cmd, shell=True, check=True)


def get_data_for_equity(args, underlying_universe_list):
    getter: DataGetter = get_source_instance(args.source, "equity")
    now = datetime.datetime.now()
    from_ms = get_ms_from_datetime(now - datetime.timedelta(days=15)) if args.live else get_ms_from_datetime(DATA_FROM)
    to_ms = get_ms_from_datetime(now if args.live else DATA_TO)
    print(from_ms, to_ms)
    print("Num of sym to get: ", len(underlying_universe_list))
    with Pool(6) as p:
        for multiplier in [5, 15] + ([] if args.inst_type == "options" else [30]):  # 1, 5, 15,
            p.starmap(
                write_ohlc, [(getter, sym, multiplier, "minute", from_ms, to_ms) for sym in underlying_universe_list]
            )
            print(f"{multiplier}min done")
        p.starmap(write_ohlc, [(getter, sym, 1, "day", from_ms, to_ms) for sym in universe_list])
        print("day done")


def get_data_for_options(args, underlying_universe_list):
    getter: DataGetter = get_source_instance(args.source, "options")
    now = datetime.datetime.now()
    if args.live:
        ticker_expiry_map = get_option_ticker_from_underlying(
            underlying_universe_list,
            now - datetime.timedelta(days=1),
            now + datetime.timedelta(weeks=3) if args.live else DATA_TO,
            num_closest_strikes=6,
        )
    else:
        ticker_expiry_map = get_option_ticker_from_underlying(underlying_universe_list, DATA_FROM, DATA_TO)

        # TODO: get the symbol and expiring, compare against to_ms
        if WRITE_NEW_SYMBOLS_ONLY:
            written_fp = (Path(os.environ["DATA_DIR"]) / f"{args.frequency}/options").glob("*_options.csv")
            written_etos = set(pd.concat([pd.read_csv(fp, usecols=["symbol"]) for fp in written_fp]).symbol.unique())
            ticker_expiry_map = dict((k, v) for k, v in ticker_expiry_map.items() if k not in written_etos)
    print("Num of sym to get: ", len(ticker_expiry_map))
    from_ms = get_ms_from_datetime(now - datetime.timedelta(days=15)) if args.live else get_ms_from_datetime(DATA_FROM)
    to_ms = get_ms_from_datetime(now if args.live else DATA_TO)
    two_day_td = pd.Timedelta(2, "d")
    to_iterate = (
        (["5minute", "15minute", "day"] + ([] if args.inst_type == "options" else ["30minute"]))
        if args.live
        else [args.frequency]
    )
    with Pool(6) as p:
        for freq in to_iterate:
            print("starting", freq)
            p.starmap(
                write_ohlc,
                [
                    (
                        getter,
                        sym,
                        int(freq.replace("minute", "")) if freq != "day" else 1,
                        "minute" if "minute" in freq else "day",
                        from_ms,
                        to_ms if args.live else get_ms_from_datetime(expiry_dt + two_day_td),
                    )
                    for sym, expiry_dt in ticker_expiry_map.items()
                ],
            )
            print(f"[{datetime.datetime.now()}] ", freq, "done")
    if not args.live:
        _store_option_data_into_history()


if __name__ == "__main__":
    args = parse_args()
    assert args.symbol is not None or args.universe is not None, "Both symbol and universe fp are none..."
    load_credentials(args.credentials, into_env=True)
    if args.universe is not None:
        universe_list = read_universe_list(args.universe)
    else:
        universe_list = args.symbol
    while True:
        # Update the bars (specific backtest code, as opposed to live trading)
        now = pd.Timestamp.now(tz=NY_TIMEZONE)
        time_since_midnight = now - now.normalize()
        if args.live and ((now.dayofweek == 4 and now.hour > 17) or now.dayofweek > 4):
            if args.inst_type == "options":
                _store_option_data_into_history()
            break
        elif args.live and (
            time_since_midnight < datetime.timedelta(hours=8)
            or time_since_midnight > datetime.timedelta(hours=17, minutes=45)
        ):
            time.sleep(get_sleep_time("60minute"))
            continue
        if args.inst_type == "equity":
            get_data_for_equity(args, universe_list)
        elif args.inst_type == "options":
            get_data_for_options(args, universe_list)
        else:
            raise Exception(f"Unaccounted inst_type: {args.inst_type}")

        if not args.live:
            break
        time.sleep(600)
