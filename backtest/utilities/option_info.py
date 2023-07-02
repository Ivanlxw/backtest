import datetime
import h5py
import os

import pandas as pd

from backtest.utilities.utils import read_option_metadata

_TICKER_INT_COLS = ["ticker", "expiration_date"]


def get_option_ticker_from_underlying(
    underlying_list, date_from: datetime.datetime, date_to: datetime.datetime, num_closest_strikes=None
) -> dict:
    # add feature to get around certain strike prices, based on underlying prices during the period
    # num_closest_strikes should only be applied to live operations
    # returns a map of available tickers to expiration date
    option_info_df = read_option_metadata()
    option_tickers = {}
    for underlying in underlying_list:
        try:
            underlying_option_info_df = option_info_df.query("underlying_sym == @underlying").copy()
            if underlying_option_info_df.empty:
                print(f"no underlying_sym df: und_sym={underlying}")
                continue
            if "expiration_date" not in underlying_option_info_df:
                print(f"expiration_date not in info_df: underlying={underlying}")
                continue
            underlying_option_info_df.expiration_date = pd.to_datetime(underlying_option_info_df.expiration_date)
            if num_closest_strikes is None:
                tickers_info = (
                    underlying_option_info_df.query("@date_from <= expiration_date <= @date_to")
                    .loc[:, _TICKER_INT_COLS]
                    .set_index(_TICKER_INT_COLS[0])
                    .squeeze()
                    .to_dict()
                )
                option_tickers.update(tickers_info)
            else:
                # TODO: Don't draw yday close price but most recent.
                underlying_yday_close_price = pd.read_csv(f"{os.environ['DATA_DIR']}/day/equity/{underlying}.csv").iloc[
                    -1
                ]["c"]
                strike_step = (
                    underlying_option_info_df.query(
                        f"contract_type == 'call' and "
                        f"expiration_date == '{underlying_option_info_df.expiration_date.iloc[-1]}'"
                    )
                    .strike_price.diff()
                    .median()
                )
                lb_strike = underlying_yday_close_price - (num_closest_strikes + 1) * strike_step
                ub_strike = underlying_yday_close_price + (num_closest_strikes + 1) * strike_step

                tickers_info = (
                    underlying_option_info_df.query(
                        "@date_from <= expiration_date <= @date_to and " f"@lb_strike <= strike_price <= @ub_strike"
                    )
                    .loc[:, _TICKER_INT_COLS]
                    .set_index(_TICKER_INT_COLS[0])
                    .squeeze()
                    .to_dict()
                )
                option_tickers.update(tickers_info)
        except KeyError:
            continue
    return option_tickers


def get_option_metadata_info(underlying_list, date_from: datetime.datetime, date_to: datetime.datetime):
    option_info_df = read_option_metadata()
    option_metadata = []
    for underlying in underlying_list:
        # underlying_key = f"/{underlying}"
        # option_info_df = pd.read_hdf(OPTION_METADATA_PATH, underlying_key)
        underlying_option_info_df = option_info_df.query("underlying_sym == @underlying").copy()
        underlying_option_info_df["expiration_date"] = pd.to_datetime(underlying_option_info_df["expiration_date"])
        underlying_option_info_df = underlying_option_info_df.query("@date_from <= expiration_date <= @date_to").copy()
        if "correction" in underlying_option_info_df:
            del underlying_option_info_df["correction"]
        option_metadata.append(underlying_option_info_df)
    return pd.concat(option_metadata)
