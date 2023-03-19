import datetime
import json
import argparse
import logging
import os
import random
from pathlib import Path

import pandas as pd

from trading.utilities.utils import timestamp_to_ms

UTILS_ABS_FP = Path(os.path.dirname(os.path.abspath(__file__)))
MODELINFO_DIR = UTILS_ABS_FP / "../../Data/strategies"
FORMAT_YYYY_MM_DD = '%y-%m-%d'
FORMAT_YYYYMMDD = '%Y%m%d'
NY_TIMEZONE = "America/New_York"
if not os.path.exists(MODELINFO_DIR):
    os.makedirs(MODELINFO_DIR, exist_ok=True)


def log_message(message: str):
    logging.info(f"{pd.Timestamp.now()}: {message}")


def parse_args():
    parser = argparse.ArgumentParser(description='Configs for running main.')
    parser.add_argument('-c', '--credentials', required=True,
                        type=Path, help="credentials filepath")
    parser.add_argument('-n', '--name', required=False, default="",
                        type=str, help="name of backtest/live strat run")
    parser.add_argument('-l', '--live', action='store_true',
                        help='inform life?')
    parser.add_argument("--num-runs", type=int, default=1,
                        help="Run backtest x times, get more aggregated performance details from log")
    parser.add_argument("--frequency", type=str, default="day",
                        help="Frequency of data. Searches a dir with same name")
    parser.add_argument("--universe", type=Path, required=True,
                        help="File path to trading universe", nargs='+')
    parser.add_argument("--start-ms", type=int, required=False,
                        help="Specific start time in ms")
    return parser.parse_args()


def remove_bs(s: str):
    # remove backslash at the end from reading from a stock_list.txt
    return s.replace("\n", "")


def read_universe(universe_fp):
    with open(universe_fp, "r") as fin:
        stock_list = fin.readlines()
    return list(map(remove_bs, stock_list))

def read_universe_list(universe_filenames):
    universe_list = []
    for universe_path in universe_filenames:
        universe_list.extend(
            read_universe(universe_path)
        )
    return universe_list


def load_credentials(credentials_fp, into_env=False) -> dict:
    credentials = {}
    with open(credentials_fp, 'r') as f:
        credentials = json.load(f)
    assert credentials, "credentials is an empty dictionary"
    if into_env:
        for k,v in credentials.items():
            os.environ[k] = v
    return credentials


def generate_start_date_in_ms(year_start, year_end) -> int:
    one_week_ago_ms = timestamp_to_ms(
        pd.Timestamp.now() - pd.Timedelta(datetime.timedelta(weeks=1)))
    time_ms = timestamp_to_ms(pd.Timestamp(year=random.randint(year_start, year_end),
                                           month=random.randint(1, 12),
                                           day=random.randint(1, 28), tz=NY_TIMEZONE))
    while time_ms > one_week_ago_ms:
        time_ms = timestamp_to_ms(pd.Timestamp(year=random.randint(year_start, year_end),
                                               month=random.randint(1, 12),
                                               day=random.randint(1, 28), tz=NY_TIMEZONE))
    return time_ms


def get_ms_from_sdate(sdate: str) -> int:
    # sdate should be in format YYYYMMDD
    return timestamp_to_ms(pd.to_datetime(datetime.datetime.strptime(sdate, FORMAT_YYYYMMDD)))

def get_ms_from_datetime(dt: datetime.datetime) -> int:
    # sdate should be in format YYYYMMDD
    return timestamp_to_ms(pd.to_datetime(dt))

def get_datetime_from_ms(arr):
    ''' arr can be vector or scalar '''
    return pd.to_datetime(arr, unit='ms')

def get_sleep_time(frequency: str):
    sleep_time_map = {
        "day": 63200,  # 12h
        "1minute": 61,     # 1m1s
        "5minute": 310,    # 5m10s
        "15minute": 480,   # 16m
        "30minute": 1900,  # 31m40s
        "60minute": 3000    # 50mins
    }
    return sleep_time_map[frequency]
