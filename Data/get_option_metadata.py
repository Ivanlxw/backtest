import argparse
import datetime
import importlib
import time
from pathlib import Path

import numpy as np
import pandas as pd
from Data.get_data import _store_option_data_into_history

from Data.source.polygon import Polygon
from backtest.utilities.utils import (
    NY_TIMEZONE,
    OPTION_METADATA_PATH,
    get_ms_from_datetime,
    load_credentials,
    read_option_metadata,
    read_universe_list,
    get_sleep_time,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Get ticker csv data via API calls to either AlphaVantage or Tiingo.")
    parser.add_argument(
        "-c",
        "--credentials",
        required=True,
        type=str,
        help="filepath to credentials.json",
    )
    parser.add_argument("--universe", type=Path, required=True, help="File path to trading universe", nargs="+")
    parser.add_argument("-l", "--live", action="store_true", help="inform life?")
    return parser.parse_args()


def get_source_instance(source):
    importlib.util.find_spec(f"Data.source.{source}")
    return importlib.import_module(f"Data.source.{source}").get_source_instance("options")


DATA_FROM = datetime.datetime(2021, 7, 15)
DATA_FROM = datetime.datetime(2023, 1, 1)
DATA_TO = datetime.datetime.now()
METADATA_COL_TYPE = {
    "cfi": "string",
    "contract_type": "string",
    "exercise_style": "string",
    "expiration_date": "datetime64[ns]",
    "primary_exchange": "string",
    "shares_per_contract": "int",
    "strike_price": "double",
    "ticker": "string",
    "correction": "double",
    "underlying_sym": "string",
}


if __name__ == "__main__":
    args = parse_args()
    load_credentials(args.credentials, into_env=True)
    universe_list = read_universe_list(args.universe)
    getter: Polygon = get_source_instance("polygon")

    future = datetime.datetime.today() + datetime.timedelta(days=12)
    to_ms = get_ms_from_datetime(future if args.live else DATA_TO)
    while True:
        now = pd.Timestamp.now(tz=NY_TIMEZONE)
        time_since_midnight = now - now.normalize()
        if args.live and now.dayofweek > 4:
            break
        elif args.live and time_since_midnight > datetime.timedelta(hours=17):
            time.sleep(1600)
            continue

        option_info_df = read_option_metadata().loc[:, METADATA_COL_TYPE.keys()]
        for underlying in universe_list:
            stored_df = option_info_df.query("underlying_sym == @underlying")
            if "expiration_date" not in stored_df.columns:
                print(f"expiration_date not in col and will cause error: {underlying}")
                continue
            from_ms = get_ms_from_datetime(datetime.datetime.now() if args.live else DATA_FROM)

            if from_ms > to_ms:
                print(f"info is updated. from_ms={from_ms} and to_ms={to_ms}")
                continue
            res = getter.get_option_info(underlying, from_ms, to_ms, not args.live)
            res_df = pd.DataFrame(res).drop(["underlying_ticker", "additional_underlyings"], axis=1, errors="ignore")
            res_df["underlying_sym"] = underlying
            if "correction" not in res_df:
                res_df["correction"] = np.nan
            if res_df.empty:
                print(f"No result for underlying={underlying}")
                continue
            if not stored_df.empty:
                underlying_info_df = (
                    res_df[METADATA_COL_TYPE.keys()]
                    .merge(stored_df, how="left", indicator=True)
                    .query("_merge == 'left_only'")
                )
                underlying_info_df = underlying_info_df.sort_values(["expiration_date", "strike_price"]).loc[
                    :, METADATA_COL_TYPE.keys()
                ]
            else:
                underlying_info_df = res_df[METADATA_COL_TYPE.keys()]
            try:
                underlying_info_df.astype(METADATA_COL_TYPE).to_csv(
                    OPTION_METADATA_PATH, mode="a", header=False, index=False
                )
            except KeyError:
                print(f"KeyError when writing metadata for {underlying}")
                continue
            break
        print("Done")
        if not args.live:
            break
        else:
            time.sleep(get_sleep_time("day"))
