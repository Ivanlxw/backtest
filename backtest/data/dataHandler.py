import datetime
import os, os.path
import pandas as pd

from abc import ABC, abstractmethod

from event import MarketEvent

class DataHandler(ABC):
    """
    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OLHCVI) for each symbol requested. 

    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """

    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        Returns last N bars from latest_symbol list, or fewer if less
        are available
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def update_bars(self,):
        """
        Push latest bar to latest symbol structure for all symbols in list
        """
        raise NotImplementedError("Should implement update_bars()")


class HistoricCSVDataHandler(DataHandler):
    """
    read CSV files from local filepath and prove inferface to
    obtain "latest" bar similar to live trading (drip feed)
    """

    def __init__(self, events, csv_dir, symbol_list, start_date, 
        end_date=None, datahandler: bool=True):
        """
        Args:
        - Event Queue on which to push MarketEvent information to
        - absolute path of the CSV files 
        - a list of symbols determining universal stocks
        """
        self.events = events ## a queue
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list
        self.start_date = start_date
        if end_date != None:
            self.end_date = end_date
        else:
            self.end_date = None
        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True

        self._open_convert_csv_files()
        if datahandler:
            self._to_generator()

    def _open_convert_csv_files(self):
        comb_index = None
        for s in self.symbol_list:
            temp = pd.read_csv(
                os.path.join(self.csv_dir, f"{s}.csv"),
                header = 0, index_col= 0,
            ).drop_duplicates()
            temp.columns = ["Open", "High", "Low", "Close", "Volume"]
            if self.start_date in temp.index:
                filtered = temp.iloc[temp.index.get_loc(self.start_date):,]
            else:
                filtered = temp
            
            if self.end_date is not None and self.end_date in temp.index:
                filtered = filtered.iloc[:temp.index.get_loc(self.end_date),]

            self.symbol_data[s] = filtered

            ## combine index to pad forward values
            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else: 
                comb_index.union(self.symbol_data[s].index.drop_duplicates())
            
            self.latest_symbol_data[s] = []
        ## reindex
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(index=comb_index, method='pad',fill_value=0)
    
    def _to_generator(self):
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].iterrows()
        
    def _get_new_bar(self, symbol):
        """
        Returns latest bar from data feed as tuple of
        (symbol, datetime, open, low, high, close, volume)
        """
        for b in self.symbol_data[symbol]:
            ## need to change strptime format depending on format of datatime in csv
            yield tuple([symbol, datetime.datetime.strptime(b[0], '%Y-%m-%d'),  ##strptime format - 
                        b[1][0], b[1][1], b[1][2], b[1][3], b[1][4]])

    def get_latest_bars(self, symbol, N=1):
        try: 
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in historical data set")
        else:
            return bars_list[-N:]
    
    def update_bars(self):
        for s in self.symbol_list:
            try:
                bar = next(self._get_new_bar(s))
            except StopIteration:
                self.continue_backtest = False
            else: 
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
        self.events.put(MarketEvent())
    
    def get_data(self):
        return self.symbol_data
