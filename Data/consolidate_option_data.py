import os
from pathlib import Path

import pandas as pd

from backtest.utilities.utils import OPTION_METADATA_PATH, parse_args, read_universe_list


def _get_eto_df_with_sym(eto_sym, freq):
    fp = Path(os.environ['DATA_DIR']) / f'{freq}/options/{eto_sym}.csv'
    if not fp.exists():
        return pd.DataFrame()
    df = pd.read_csv(fp)
    df['symbol'] = eto_sym
    return df


def consolidate_option_sym_df(underlying_list, frequency):
    base_universe_dir = Path(os.environ['DATA_DIR'])
    for und in underlying_list:
        try:
            und_option_md_df = pd.read_hdf(OPTION_METADATA_PATH, f'/{und}')
        except KeyError: # TODO remove once block is successful
            print(f"{und} has no metadata info")
            continue
        df = pd.concat([_get_eto_df_with_sym(eto_sym, frequency) for eto_sym in und_option_md_df.ticker.unique()])
        if df.empty:
            continue
        try:
            fp_to_write = base_universe_dir / f'{frequency}/options/{und}_options.csv'
            df = df.sort_values(['symbol', 't'])
            if fp_to_write.exists():
                existing_df = pd.read_csv(fp_to_write)
                df = pd.concat([existing_df[df.columns], df]).drop_duplicates()
            if not df.empty:
                df.to_csv(fp_to_write, index=False)
        except Exception as e:
            print(f"Exception: {und}\n{df.head()}")
            print(e)
            raise Exception()

        for eto_sym in und_option_md_df.ticker.unique():
            fp = base_universe_dir / f'{frequency}/{eto_sym}.csv'
            if fp.exists():
                fp.unlink()
    

if __name__ == "__main__":
    args = parse_args()
    universe_list = read_universe_list(args.universe)
    consolidate_option_sym_df(universe_list, args.frequency)