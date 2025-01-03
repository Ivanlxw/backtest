import datetime
import json
import argparse
import logging
import os
import random
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

from trading.utilities.utils import FORMAT_YYYYMMDD, NY_TIMEZONE, timestamp_to_ms

UTILS_ABS_FP = Path(os.path.dirname(os.path.abspath(__file__)))
MODELINFO_DIR = UTILS_ABS_FP / "../../Data/strategies"
OPTION_METADATA_PATH = Path(f"{os.environ['DATA_DIR']}/options/metadata.csv.gz")
DATA_GETTER_INST_TYPES = ['equity', 'options']
DATA_GETTER_DEFAULT_START_DT = datetime.datetime(2019, 6, 1)



def log_message(message: str):
    logging.info(f"{pd.Timestamp.now()}: {message}")


def parse_args():
    parser = argparse.ArgumentParser(description='Configs for running main.')
    parser.add_argument('-l', '--live', action='store_true', default=False,
                        help='inform life?')
    parser.add_argument("--inst-type", type=str, required=False,
                        default='equity', choices=DATA_GETTER_INST_TYPES)
    parser.add_argument("--num-runs", type=int, default=1,
                        help="Run backtest x times, get more aggregated performance details from log")
    parser.add_argument("--start-ms", type=int, required=False,
                        help="Specific start time in ms")
    parser.add_argument("--config-name", type=str, required=True)
    return parser.parse_args()


def remove_bs(s: str):
    # remove backslash at the end from reading from a stock_list.txt
    return s.replace("\n", "")

def get_db_engine():
    return create_engine(os.environ['DB_URL'])

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
        for k, v in credentials.items():
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


def _set_ram_limits(memory_in_kb):
    import resource
    soft, _ = resource.getrlimit(resource.RLIMIT_AS)
    # Convert KiB to bytes
    resource.setrlimit(resource.RLIMIT_AS, (memory_in_kb * 1024, soft))
