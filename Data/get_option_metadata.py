import argparse
import datetime
import importlib
from pathlib import Path

import pandas as  pd

from Data.source.polygon import Polygon
from backtest.utilities.utils import OPTION_METADATA_PATH, get_ms_from_datetime, load_credentials, read_universe_list

def parse_args():
    parser = argparse.ArgumentParser(
        description="Get ticker csv data via API calls to either AlphaVantage or Tiingo."
    )
    parser.add_argument("-c", "--credentials", required=True, type=str, help="filepath to credentials.json",)
    parser.add_argument("--universe", type=Path,  required=True, help="File path to trading universe", nargs="+")
    parser.add_argument('-l', '--live', action='store_true', help='inform life?')
    return parser.parse_args()

def get_source_instance(source, universe_list):
    importlib.util.find_spec(f"Data.source.{source}")
    return importlib.import_module(f"Data.source.{source}").get_source_instance(
        universe_list, "options")

DATA_FROM = datetime.datetime(2019, 10, 1)
DATA_TO = datetime.datetime(2023, 4, 30)

if __name__ == "__main__":
    args = parse_args()
    load_credentials(args.credentials, into_env=True)
    universe_list = read_universe_list(args.universe)
    getter: Polygon = get_source_instance("polygon", universe_list)

    today = datetime.datetime.today()
    future = today + datetime.timedelta(days=10)
    from_ms = get_ms_from_datetime(today if args.live else DATA_FROM)
    to_ms = get_ms_from_datetime(future if args.live else DATA_TO)
    print(from_ms, to_ms)
    res = getter.get_option_info(from_ms, to_ms, not args.live)
    res_df = pd.DataFrame(res)
    for underlying in getter.universe_list:
        underlying_info_df = res_df.query(f"underlying_ticker == '{underlying}'").drop('underlying_ticker', axis=1)
        if underlying_info_df.empty:
            continue
        underlying_key = f"/{underlying}"
        try:
            stored_df = pd.read_hdf(OPTION_METADATA_PATH, underlying_key)
            underlying_info_df = underlying_info_df.drop("additional_underlyings", axis=1).merge(
                stored_df.drop("additional_underlyings", axis=1), how='outer').sort_values(['expiration_date', 'strike_price'])
            underlying_info_df.to_hdf(OPTION_METADATA_PATH, underlying_key)
        except KeyError:
            underlying_info_df.to_hdf(OPTION_METADATA_PATH, underlying_key)
        except Exception as e:
            raise Exception(f"Unexpected exception: {e}")
