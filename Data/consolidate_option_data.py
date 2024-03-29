import concurrent.futures as fut
import os
from pathlib import Path

import pandas as pd
from backtest.utilities.option_info import OptionMetadata
from backtest.utilities.utils import parse_args, read_universe_list


def _get_eto_df_with_sym(eto_sym, freq):
    fp = Path(os.environ["DATA_DIR"]) / f"{freq}/options_raw/{eto_sym}.csv"
    if not fp.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(fp)
        df["symbol"] = eto_sym
        return df
    except Exception as e:
        print(f"Exception on {fp}")
        print(e)
        return pd.DataFrame()


def consolidate_option_sym_df(underlying_list, frequency):
    base_universe_dir = Path(os.environ["DATA_DIR"])
    for und in underlying_list:
        print(und)
        underlying_option_metadata_df = OptionMetadata().get_option_metadata_for_symbol(und)
        unique_tickers = underlying_option_metadata_df.ticker.unique()
        if len(unique_tickers) == 0:
            continue
        res = []
        with fut.ThreadPoolExecutor(8) as exec:
            res = [exec.submit(_get_eto_df_with_sym, eto_sym, frequency) for eto_sym in unique_tickers]
            res = [r.result() for r in res]
        df = pd.concat(res)
        if df.empty:
            continue
        try:
            fp_to_write = base_universe_dir / f"{frequency}/options/{und}_options.csv"
            df = df.sort_values(["symbol", "timestamp"])
            if fp_to_write.exists():
                existing_df = pd.read_csv(fp_to_write)
                df = pd.concat([existing_df[df.columns], df]).drop_duplicates(keep='last')
            if not df.empty:
                df.to_csv(fp_to_write, index=False)
        except Exception as e:
            print(f"Exception: {und}\n{df.head()}")
            print(e)
            raise Exception()

        for eto_sym in underlying_option_metadata_df.ticker.unique():
            fp = base_universe_dir / f"{frequency}/options_raw/{eto_sym}.csv"
            if fp.exists():
                fp.unlink()


if __name__ == "__main__":
    args = parse_args()
    universe_list = read_universe_list(args.universe)
    consolidate_option_sym_df(universe_list, args.frequency)
