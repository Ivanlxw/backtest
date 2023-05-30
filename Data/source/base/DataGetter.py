import abc
import os
from pathlib import Path
from typing import List

import pandas as pd

from backtest.utilities.utils import DATA_GETTER_INST_TYPES


class DataGetter(abc.ABC):
    def __init__(self, inst_type) -> None:
        self.RAW_DATA_DIR = Path(os.getenv("DATA_DIR"))
        assert inst_type in DATA_GETTER_INST_TYPES, f"only inst_type from {DATA_GETTER_INST_TYPES} are allowed. inst_type={inst_type}"
        self.inst_type = inst_type
        self._data_getter_options = ["csv", "gz", "proto"]

    def get_all_methods(self):
        return list(object.__dict__.keys())
    
    def get_fp(self, freq_prefix, freq, symbol, compression="csv"):
        assert compression in ['csv', 'csv.gz'], "Only csv and csv.gz allowed"
        return self.RAW_DATA_DIR / f"{freq_prefix}{freq}" / self.inst_type / f"{symbol}.{compression}"

    @abc.abstractmethod
    def get_ohlc(self, symbol, multiplier, freq, from_ms, to_ms):
        return

    def write_ohlc(self, symbol: str, multiplier, freq, from_ms, to_ms, fp, compression="csv"):
        assert compression in self._data_getter_options, f"compression has to be one of {self._data_getter_options}"

        df = self.get_ohlc(symbol, multiplier, freq, from_ms, to_ms)
        if compression == "proto":
            # TODO: Implement
            pass
        else:
            if df.empty:
                return
            df = df[~df.index.duplicated(keep='last')]
            if fp.exists():
                try:
                    df = pd.concat([pd.read_csv(fp, index_col=0), df])
                    df = df[~df.index.duplicated(keep='last')].sort_index()
                except Exception as e:
                    print(f"Exception for {symbol}: {e}")
                    return
            df.to_csv(fp)
