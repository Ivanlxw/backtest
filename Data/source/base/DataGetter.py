import abc
import os
from pathlib import Path
from typing import List

import pandas as pd

from backtest.utilities.utils import DATA_GETTER_INST_TYPES


class DataGetter(abc.ABC):
    def __init__(self, universe_list: List[str], inst_type) -> None:
        self.RAW_DATA_DIR = Path(os.getenv("DATA_DIR"))
        self.universe_list = universe_list
        assert inst_type in DATA_GETTER_INST_TYPES, f"only inst_type from {DATA_GETTER_INST_TYPES} are allowed. inst_type={inst_type}"
        self.inst_type = inst_type
        self._data_getter_options = ["csv", "gz", "proto"]

    def get_all_methods(self):
        return list(object.__dict__.keys())

    @abc.abstractmethod
    def get_ohlc(self, symbol, multiplier, freq, from_ms, to_ms):
        return

    def write_ohlc(self, symbol: str, multiplier, freq, from_ms, to_ms, compression="csv"):
        assert compression in self._data_getter_options, f"compression has to be one of {self._data_getter_options}"

        df = self.get_ohlc(symbol, multiplier, freq, from_ms, to_ms)
        if compression == "proto":
            # TODO: Implement
            pass
        else:
            if df.empty:
                return
            df = df[~df.index.duplicated(keep='last')]
            suffix_after_csv = ".gz" if compression == "gz" else ""
            freq_prefix = "" if freq == "day" else multiplier
            fp = self.RAW_DATA_DIR / f"{freq_prefix}{freq}" / f"{symbol}.csv{suffix_after_csv}"
            if fp.exists():
                df = pd.concat([pd.read_csv(fp, index_col=0), df])
                df = df[~df.index.duplicated(keep='last')].sort_index()
            df.to_csv(fp)
