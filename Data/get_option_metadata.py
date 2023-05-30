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

def get_source_instance(source):
    importlib.util.find_spec(f"Data.source.{source}")
    return importlib.import_module(f"Data.source.{source}").get_source_instance("options")

DATA_FROM = datetime.datetime(2019, 10, 1)
DATA_TO = datetime.datetime.now()

if __name__ == "__main__":
    args = parse_args()
    load_credentials(args.credentials, into_env=True)
    universe_list = read_universe_list(args.universe)
    getter: Polygon = get_source_instance("polygon")

    today = datetime.datetime.today()
    future = today + datetime.timedelta(days=12)
    to_ms = get_ms_from_datetime(future if args.live else DATA_TO)
    
    for underlying in universe_list:
        underlying_key = f"/{underlying}"
        try:
            stored_df = pd.read_hdf(OPTION_METADATA_PATH, underlying_key)
            from_ms = get_ms_from_datetime(pd.to_datetime(stored_df.expiration_date.sort_values().iloc[-1]).to_pydatetime())
            # from_ms = get_ms_from_datetime(DATA_FROM)
        except KeyError:
            print(f"KeyError when reading metadata for {underlying}")
            continue
        except Exception as e:
            raise Exception(f"Unexpected exception: {e}")
        if from_ms > to_ms:
            print(f"info is updated. from_ms={from_ms} and to_ms={to_ms}")
            continue
        res = getter.get_option_info(underlying, from_ms, to_ms, not args.live)
        res_df = pd.DataFrame(res).drop(['underlying_ticker', 'additional_underlyings'], axis=1, errors='ignore')
        if res_df.empty:
            print(f"No result for underlying={underlying}")
            continue
        underlying_info_df = res_df.merge(stored_df.drop("additional_underlyings", axis=1, errors='ignore'), 
                                          how='outer').sort_values(['expiration_date', 'strike_price'])
        try:
            underlying_info_df.to_hdf(OPTION_METADATA_PATH, underlying_key)
        except KeyError:
            print(f"KeyError when writing metadata for {underlying}")
            continue
