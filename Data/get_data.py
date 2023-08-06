import argparse
import concurrent.futures as fut
import datetime
import importlib
import os
from pathlib import Path
import subprocess
import time

import pandas as pd

from Data.source.base.DataGetter import OHLC_COLUMNS, DataGetter, get_ohlc_fp
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
DATA_FROM = datetime.datetime(2021, 8, 15)
DATA_TO = datetime.datetime(2023, 8, 1)
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


def _write_ohlc(symbol, multiplier, time_scale, inst_type, df):
    ohlc_types = {
        "timestamp": "int64",
        "volume": "float64",
        "vwap": "float64",
        "open": "float64",
        "close": "float64",
        "high": "float64",
        "low": "float64",
        "num_trades": "float64",
    }
    if df.empty:
        return
    fp = get_ohlc_fp(multiplier, time_scale, symbol, inst_type)
    if fp.exists():
        try:
            existing_df = pd.read_csv(fp).astype(ohlc_types)
            df = df.astype(ohlc_types)
        except Exception as e:
            print(f"Could not write into {fp}:\n{e}")
            return
        df = pd.concat([existing_df, df]).drop_duplicates(keep="last")
    df.set_index('timestamp', inplace=True, drop=True)
    df.loc[:, OHLC_COLUMNS].to_csv(fp)


def _store_option_data_into_history(freq: str):
    proj_root_dir = os.environ["WORKSPACE_ROOT"]
    cmd = (
        f"python {proj_root_dir}/Data/consolidate_option_data.py --credentials {proj_root_dir}/real_credentials.json "
        f"--universe {' '.join([str(univ_path) for univ_path in args.universe])} --frequency {freq}"
    )
    subprocess.run(cmd, shell=True, check=True)


def get_data_for_equity(args, underlying_universe_list):
    getter: DataGetter = get_source_instance(args.source, "equity")
    now = datetime.datetime.now()
    from_ms = get_ms_from_datetime(now - datetime.timedelta(days=25)) if args.live else get_ms_from_datetime(DATA_FROM)
    to_ms = get_ms_from_datetime(now if args.live else DATA_TO)
    print(from_ms, to_ms)
    print("Num of sym to get: ", len(underlying_universe_list))
    to_iterate = ["5minute", "15minute", "30minute", "day"] if args.live else [args.frequency]
    for freq in to_iterate:
        sym_and_time_list = [
            [(start_ms, end_ms, sym) for start_ms, end_ms in getter.equity_chop_dates(from_ms, to_ms, freq)]
            if args.source == "polygon"
            else [(from_ms, to_ms, sym)]
            for sym in underlying_universe_list
        ]
        sym_and_time_list = [entry for combi in sym_and_time_list for entry in combi]
        multiplier = int(freq.replace("minute", "")) if freq != "day" else 1
        time_scale = "minute" if "minute" in freq else "day"
        with fut.ThreadPoolExecutor(16) as p:
            res_df = [
                p.submit(getter.get_ohlc, sym, multiplier, time_scale, start_ms, end_ms)
                for start_ms, end_ms, sym in sym_and_time_list
            ]
            res_df = [r.result() for r in res_df]
        for (_, _, sym), df in zip(sym_and_time_list, res_df):
            _write_ohlc(sym, multiplier, time_scale, "equity", df)
        print(f"[{datetime.datetime.now()}] ", freq, "done")


def get_data_for_options(args, underlying_universe_list):
    getter: DataGetter = get_source_instance(args.source, "options")
    now = datetime.datetime.now()
    if args.live:
        ticker_expiry_list = [
            (sym, expiry_dt)
            for sym, expiry_dt in get_option_ticker_from_underlying(
                underlying_universe_list,
                now - datetime.timedelta(days=3),
                now + datetime.timedelta(days=10),
                num_closest_strikes=6,
            ).items()
        ]
    else:
        ticker_expiry_list = [
            (sym, expiry_dt)
            for sym, expiry_dt in get_option_ticker_from_underlying(
                underlying_universe_list, DATA_FROM, DATA_TO
            ).items()
        ]
        # TODO: get the symbol and expiring, compare against to_ms
        if WRITE_NEW_SYMBOLS_ONLY:
            written_fp = list((Path(os.environ["DATA_DIR"]) / f"{args.frequency}/options").glob("*_options.csv"))
            if len(written_fp) != 0:
                written_etos = set(pd.concat([pd.read_csv(fp, usecols=["symbol"]) for fp in written_fp]).symbol.unique())
                ticker_expiry_list = [(k, v) for k, v in ticker_expiry_list if k not in written_etos]
    print("Num of sym to get: ", len(ticker_expiry_list))
    to_iterate = ["5minute", "15minute", "day"] if args.live else [args.frequency]
    chunk = 12000
    ticker_expiry_chunks = [
        ticker_expiry_list[i * chunk : (i + 1) * chunk] for i in range((len(ticker_expiry_list) + chunk - 1) // chunk)
    ]
    del ticker_expiry_list
    for freq in to_iterate:
        print("starting", freq)
        multiplier = int(freq.replace("minute", "")) if freq != "day" else 1
        time_scale = "minute" if "minute" in freq else "day"
        for ticker_expiry_chunk in ticker_expiry_chunks:
            with fut.ThreadPoolExecutor(32) as p:
                res_df = [
                    p.submit(
                        getter.get_ohlc,
                        sym,
                        multiplier,
                        time_scale,
                        get_ms_from_datetime(expiry_dt - pd.Timedelta(100, "d")),
                        get_ms_from_datetime(expiry_dt + pd.Timedelta(2, "d")),
                    )
                    for sym, expiry_dt in ticker_expiry_chunk
                ]
                res_df = [r.result() for r in res_df]
            for (sym, _), df in zip(ticker_expiry_chunk, res_df):
                _write_ohlc(sym, multiplier, time_scale, "options", df)
        print(f"[{datetime.datetime.now()}] ", freq, "done")
    if not args.live:
        _store_option_data_into_history(freq)


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
        if args.live and now.dayofweek > 4:
            if args.inst_type == "options":
                for freq in ["5minute", "15minute", "day"]:
                    _store_option_data_into_history(freq)
            break
        # elif args.live and (
        #     time_since_midnight < datetime.timedelta(hours=8)
        #     or time_since_midnight > datetime.timedelta(hours=17, minutes=45)
        # ):
        #     time.sleep(get_sleep_time("60minute"))
        #     continue
        if args.inst_type == "equity":
            get_data_for_equity(args, universe_list)
        elif args.inst_type == "options":
            get_data_for_options(args, universe_list)
        else:
            raise Exception(f"Unaccounted inst_type: {args.inst_type}")

        if not args.live:
            break
        time.sleep(600)
