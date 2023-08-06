import datetime
import os

import pandas as pd

from backtest.utilities.utils import OPTION_METADATA_PATH

_TICKER_INT_COLS = ["ticker", "expiration_date"]


class OptionMetadata:
    DATA_INITIALIZED = False

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(OptionMetadata, cls).__new__(cls)
        return cls.instance

    def _initialize(self):
        if not self.DATA_INITIALIZED:
            self.data: pd.DataFrame = pd.read_csv(OPTION_METADATA_PATH, index_col=None)
            self.DATA_INITIALIZED = True

    def get_option_metadata(self):
        self._initialize()
        return self.data.copy()

    def get_option_metadata_for_symbol(self, symbol):
        self._initialize()
        return self.data.query("underlying_sym == @symbol").copy()


def get_option_ticker_from_underlying(
    underlying_list, date_from: datetime.datetime, date_to: datetime.datetime, num_closest_strikes=None
) -> dict:
    # add feature to get around certain strike prices, based on underlying prices during the period
    # num_closest_strikes should only be applied to live operations
    # returns a map of available tickers to expiration date
    option_tickers = {}
    for underlying in underlying_list:
        try:
            underlying_option_info_df = OptionMetadata().get_option_metadata_for_symbol(underlying)
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
                ]["close"]
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
        except KeyError as e:
            print("Keyerror", e)
            continue
    return option_tickers


def get_option_metadata_info(underlying_list, date_from: datetime.datetime, date_to: datetime.datetime):
    # option_metadata = []
    bato = 'BATO'
    # for underlying in underlying_list:
    underlying_option_info_df = OptionMetadata().get_option_metadata().query(
        "underlying_sym in @underlying_list and primary_exchange == @bato"
    )
    underlying_option_info_df["expiration_date"] = pd.to_datetime(underlying_option_info_df["expiration_date"])
    underlying_option_info_df = underlying_option_info_df.query("@date_from <= expiration_date <= @date_to").copy()
    if "correction" in underlying_option_info_df:
        del underlying_option_info_df["correction"]
    return underlying_option_info_df
    # option_metadata.append(underlying_option_info_df)
    # return pd.concat(option_metadata)
