import datetime
import json
import argparse
import logging
import os
import random
from pathlib import Path
from typing import List

import pandas as pd

from trading.utilities.utils import timestamp_to_ms

UTILS_ABS_FP = Path(os.path.dirname(os.path.abspath(__file__)))
MODELINFO_DIR = UTILS_ABS_FP / "../../Data/strategies"
FORMAT_YYYY_MM_DD = '%y-%m-%d'
FORMAT_YYYYMMDD = '%Y%m%d'
if not os.path.exists(MODELINFO_DIR):
    os.makedirs(MODELINFO_DIR, exist_ok=True)


def log_message(message: str):
    logging.info(f"{pd.Timestamp.now()}: {message}")


def parse_args():
    parser = argparse.ArgumentParser(description='Configs for running main.')
    parser.add_argument('-c', '--credentials', required=True,
                        type=str, help="credentials filepath")
    parser.add_argument('-n', '--name', required=False, default="",
                        type=str, help="name of backtest/live strat run")
    parser.add_argument('-l', '--live', action='store_true',
                        help='inform life?')
    parser.add_argument("--num-runs", type=int, default=1,
                        help="Run backtest x times, get more aggregated performance details from log")
    parser.add_argument("--frequency", type=str, default="daily",
                        help="Frequency of data. Searches a dir with same name")
    parser.add_argument("--universe", type=Path, required=True,
                        help="File path to trading universe", nargs='+')
    parser.add_argument("--start-ms", type=int, required=False,
                        help="Specific start time in ms")
    return parser.parse_args()


def remove_bs(s: str):
    # remove backslash at the end from reading from a stock_list.txt
    return s.replace("\n", "")


def get_etf_list(base_dir: Path):
    with open(base_dir / "Data/universe/etf.txt") as fin:
        l = list(map(remove_bs, fin.readlines()))
    return l


def get_snp500_list(base_dir: Path):
    with open(base_dir / "Data/universe/snp500.txt") as fin:
        l = list(map(remove_bs, fin.readlines()))
    return l


def get_us_stocks(base_dir: Path):
    with open(base_dir / "Data/universe/us_stocks.txt") as fin:
        l += list(map(remove_bs, fin.readlines()))
    return l


def get_universe(base_dir: Path):
    sym_filenames = ["dow.txt", "snp100.txt",
                     "snp500.txt", "nasdaq.txt", "etf.txt"]
    l = []
    for file in sym_filenames:
        with open(base_dir / "Data/universe" / file) as fin:
            l += list(map(remove_bs, fin.readlines()))
    return l


def get_trading_universe(fp_list: List[Path]) -> set:
    l = []
    for fp in fp_list:
        with open(fp) as fin:
            l += list(map(remove_bs, fin.readlines()))
    return set(l)


def load_credentials(credentials_fp) -> dict:
    credentials = {}
    with open(credentials_fp, 'r') as f:
        credentials = json.load(f)
    assert credentials, "credentials is an empty dictionary"
    return credentials


def generate_start_date_in_ms(year_start, year_end) -> int:
    one_week_ago_ms = timestamp_to_ms(
        pd.Timestamp.now() - pd.Timedelta(datetime.timedelta(weeks=1)))
    time_ms = timestamp_to_ms(pd.Timestamp(year=random.randint(year_start, year_end),
                                           month=random.randint(1, 12),
                                           day=random.randint(1, 28)))
    while time_ms > one_week_ago_ms:
        time_ms = timestamp_to_ms(pd.Timestamp(year=random.randint(year_start, year_end),
                                               month=random.randint(1, 12),
                                               day=random.randint(1, 28)))
    return time_ms


def get_ms_from_sdate(sdate: str) -> int:
    # sdate should be in format YYYYMMDD
    return timestamp_to_ms(pd.to_datetime(datetime.datetime.strptime(sdate, FORMAT_YYYYMMDD)))


def get_sleep_time(frequency: str):
    sleep_time_map = {
        "daily": 43200,  # 12h
        "1min": 61,     # 1m1s
        "5min": 310,    # 5m10s
        "15min": 480,   # 16m
        "30min": 1900,  # 31m40s
    }
    return sleep_time_map[frequency]
