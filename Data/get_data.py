import argparse
import datetime
import importlib
from multiprocessing import Pool
import os
from pathlib import Path
import random
import subprocess
import time


import pandas as pd

from Data.source.base.DataGetter import DataGetter
from backtest.utilities.inst import get_option_ticker_from_underlying
from backtest.utilities.utils import DATA_GETTER_DEFAULT_END_DT, DATA_GETTER_INST_TYPES, NY_TIMEZONE, get_ms_from_datetime, load_credentials, get_sleep_time, read_universe_list

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
    parser.add_argument("--frequency", type=str, default="day",
                        help="Frequency of data. Searches a dir with same name")
    parser.add_argument('-l', '--live', action='store_true',
                        help='inform life?')
    return parser.parse_args()


def get_source_instance(source, inst_type: str):
    importlib.util.find_spec(f"Data.source.{source}")
    return importlib.import_module(f"Data.source.{source}").get_source_instance(inst_type)


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

def write_ohlc(getter: DataGetter, symbol, multiplier, freq, from_ms, to_ms):
    '''min(len(universe_list), 25)
        from_ms: None if want to infer from existing dataset
    '''
    fp = getter.get_fp("" if freq == "day" else multiplier, freq, symbol)
    if fp.exists() and from_ms is None:
        existing_df = pd.read_csv(fp)
        from_ms = existing_df.sort_values('t')['t'].iloc[-1]
    if from_ms is None:
        from_ms = get_ms_from_datetime(DATA_FROM)
    getter.write_ohlc(symbol, multiplier, freq, from_ms, to_ms, fp)


# change this from intended start date: datetime.datetime(2022, 9, 1)
DATA_FROM = datetime.datetime(2020, 6, 1) # DATA_GETTER_DEFAULT_START_DT
DATA_TO = DATA_GETTER_DEFAULT_END_DT + datetime.timedelta(weeks=3)
DERIVE_START_MS_FROM_FILE = False
WRITE_NEW_SYMBOLS_ONLY = True

if __name__ == "__main__":
    args = parse_args()
    load_credentials(args.credentials, into_env=True)
    universe_list = read_universe_list(args.universe)
    if args.inst_type == "options" and not args.live:
        underlying_list = universe_list
        universe_list = random.sample(universe_list, min(len(universe_list), 150))
        universe_list = get_option_ticker_from_underlying(universe_list,
                                                          datetime.datetime.now() if args.live else DATA_FROM,
                                                          DATA_TO)
    getter: DataGetter = get_source_instance(args.source, args.inst_type)
    if not args.live or (args.inst_type == "options" and WRITE_NEW_SYMBOLS_ONLY):
        written_fp = (Path(os.environ['DATA_DIR']) / "day/options").glob('*_options.csv')
        written_etos = set(pd.concat([pd.read_csv(fp, usecols=['symbol']) for fp in written_fp]).symbol.unique())
        universe_list = [sym for sym in universe_list if sym not in written_etos]
        print(universe_list)
    print("Num of sym to get: ", len(universe_list))
    processes = []
    while True:
        # Update the bars (specific backtest code, as opposed to live trading)
        now = pd.Timestamp.now(tz=NY_TIMEZONE)
        time_since_midnight = now - now.normalize()
        if args.live and ((now.dayofweek == 4 and now.hour > 17) or now.dayofweek > 4):
            break
        elif args.live and (time_since_midnight < datetime.timedelta(hours=8) or time_since_midnight > datetime.timedelta(hours=17, minutes=45)):
            time.sleep(60)
            continue
        day_history = now - datetime.timedelta(days=35)
        intraday_hist = now - datetime.timedelta(days=7)
        from_ms = get_ms_from_datetime(day_history) if args.live else (None if DERIVE_START_MS_FROM_FILE else get_ms_from_datetime(DATA_FROM))
        to_ms = get_ms_from_datetime(DATA_TO)
        print(from_ms, to_ms)
        with Pool(12) as p:
            R_day = p.starmap(write_ohlc, [(getter, sym, 1, "day", from_ms, to_ms) for sym in universe_list])
            if args.inst_type == "equity":
                for multiplier in [5, 30, 60]:  # 1, 5, 15,
                    R_intraday = p.starmap(write_ohlc, [(getter, sym, multiplier, "minute", from_ms, to_ms) for sym in universe_list])
                    print(f"{multiplier}min done")
        if args.inst_type == "options":
            proj_root_dir = os.environ['WORKSPACE_ROOT']
            cmd = (f"python {proj_root_dir}/Data/consolidate_option_data.py --credentials {proj_root_dir}/real_credentials.json "
                   f"--universe {' '.join([str(univ_path) for univ_path in args.universe])}")
            subprocess.run(cmd, shell=True, check=True)
        if not args.live:
            break
        time.sleep(get_sleep_time("15minute"))
