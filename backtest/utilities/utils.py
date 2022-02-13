import json
import argparse
from pathlib import Path
import logging
import os
import random
import pandas as pd

UTILS_ABS_FP = Path(os.path.dirname(os.path.abspath(__file__)))
MODELINFO_DIR = UTILS_ABS_FP / "../../Data/strategies"
if not os.path.exists(MODELINFO_DIR):
    os.makedirs(MODELINFO_DIR, exist_ok=True)


def parse_args():
    parser = argparse.ArgumentParser(description='Configs for running main.')
    parser.add_argument('-c', '--credentials', required=True,
                        type=str, help="credentials filepath")
    parser.add_argument('-n', '--name', required=False, default="",
                        type=str, help="name of backtest/live strat run")
    parser.add_argument('-l', '--live', action='store_true', help='inform life?')
    parser.add_argument("--num-runs", type=int, default=1, help="Run backtest x times, get more aggregated performance details from log")
    parser.add_argument("--frequency", type=str, default="daily", help="Frequency of data. Searches a dir with same name")
    parser.add_argument("--sleep-time", type=int, default=43200, help="Sleep time in seconds. Defaults to sleep time in live loop")
    return parser.parse_args()


def remove_bs(s: str):
    # remove backslash at the end from reading from a stock_list.txt
    return s.replace("\n", "")


def load_credentials(credentials_fp):
    with open(credentials_fp, 'r') as f:
        credentials = json.load(f)
        for k, v in credentials.items():
            os.environ[k] = v


def generate_start_date():
    return "{}-{:02d}-{:02d}".format(
        random.randint(2010, 2018),
        random.randint(1, 12),
        random.randint(1, 28)
    )

def generate_start_date_after_2015():
    return "{}-{:02d}-{:02d}".format(
        random.randint(2015, 2019),
        random.randint(1, 12),
        random.randint(1, 28)
    )

def log_message(message: str):
    logging.info(f"{pd.Timestamp.now()}: {message}")
