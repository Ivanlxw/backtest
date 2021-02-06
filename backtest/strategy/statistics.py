# import os, sys
# sys.path.append((os.path.dirname(os.path.abspath(__file__))))  ## 2 dirs above

import numpy as np
import pandas as pd
from abc import ABCMeta, abstractmethod
from sklearn.preprocessing import LabelEncoder

from backtest.strategy.naive import Strategy
from backtest.event import SignalEvent
from backtest.utilities.enums import OrderPosition

class BuyDips(Strategy):
    def __init__(self, bars, events, short_time, long_time) -> None:
        self.bars = bars
        self.events = events
        self.st = short_time
        self.lt = long_time
    
    def calculate_signals(self, event):
        if event.type == 'MARKET':
            # corr_diff = dict((sym, None) for sym in self.bars.symbol_list)
            for sym in self.bars.symbol_list:
                temp_data = np.array(self.bars.get_latest_bars(sym, N=self.lt))
                # price_pct_change = np.diff(temp_data[:, 5].astype('float32')) / temp_data[1:, 5].astype('float32') * 100
                if len(temp_data) != self.lt:
                    continue
                lt_corr = np.corrcoef(range(self.lt), temp_data[:, 5].astype('float32'))
                # st_corr = np.corrcoef(range(self.st), temp_data[self.lt-self.st:, 5].astype('float32'))
                if lt_corr[0][1] > 0.50 and np.percentile(temp_data[:, 5], 5) > temp_data[-1,5]: 
                    self.put_to_queue_(temp_data[-1][0], temp_data[-1][1], OrderPosition.BUY)
                elif lt_corr[0][1] < 0.50 and np.percentile(temp_data[:, 5], 90) < temp_data[-1,5]:
                    self.put_to_queue_(temp_data[-1][0], temp_data[-1][1], OrderPosition.SELL)

                if lt_corr[0][1] > 0.80 and np.percentile(temp_data[self.lt-self.st:, 5], 10) > temp_data[-1,5] :
                    self.put_to_queue_(temp_data[-1][0], temp_data[-1][1], OrderPosition.SELL)
                elif lt_corr[0][1] < -0.80 and np.percentile(temp_data[self.lt-self.st:, 5], 85) < temp_data[-1,5]:
                    self.put_to_queue_(temp_data[-1][0], temp_data[-1][1], OrderPosition.SELL)
                

class StatisticalStrategy():
    def __init__(self, bars, events, model, processor):
        self.events = events
        self.columns = ["symbol", "date", "Open", "High", "Low", "Close", "Volume"]
        self.model = model
        self.bars = bars
        self.symbol_list = self.bars.symbol_list
        self.processor = processor

    @abstractmethod
    def optimize(self,):
        raise NotImplementedError("Must implement optimize()")
    
    def _prepare_flow_data(self, bars):
        temp_df = pd.DataFrame(bars, columns = self.columns)
        temp_df = temp_df.drop(["symbol", "date"], axis=1)
        return self.model.predict(self.processor.preprocess_X(temp_df))
    

## Sklearn regressor (combined)
class RawRegression(StatisticalStrategy, Strategy):
    def __init__(self, bars, events, model, processor, reoptimize_days:int):
        StatisticalStrategy.__init__(self, bars, events, model, processor)
        self.model = {}
        self.reg = model
        self.reoptimize_days = reoptimize_days
        for sym in self.bars.symbol_list:
            self.model[sym] = None

    def optimize(self):
        '''
        We have unique models for each symbol and fit each one of them. Not ideal for large stocklist
        '''
        ## data is dict of ndarray
        for sym in self.bars.symbol_list:
            temp_data = self.bars.get_latest_bars(sym, N=self.reoptimize_days)
            data = pd.DataFrame(temp_data, columns=self.columns)
            data.set_index('date', inplace=True)
            data.drop('symbol', axis=1, inplace=True)
            X, y = self.processor.process_data(data)
            if self.model[sym] is None and not (X["Close"]==0).all():
                self.model[sym] = self.reg()
                self.model[sym].fit(X,y)
            
    def _prepare_flow_data(self, bars, sym):
        temp_df = pd.DataFrame(bars, columns = self.columns)
        temp_df = temp_df.drop(["symbol", "date"], axis=1)
        return None if self.model[sym] is None else self.model[sym].predict(self.processor._transform_X(temp_df))

    def calculate_signals(self, event):
        if event.type == "MARKET":
            for s in self.symbol_list:
                bars_list = self.bars.get_latest_bars(s, N=self.processor.get_shift())
                if len(bars_list) < self.processor.get_shift() or \
                    bars_list[-1][-1] == 0.0:
                    return

                ## this is slow and should be optimized.
                preds = self._prepare_flow_data(bars_list, s)
                if preds is None:
                    return
                if preds[-1] > 0.07:
                    self.put_to_queue_(bars_list[-1][0], bars_list[-1][1], OrderPosition.BUY)
                elif preds[-1] < -0.07:
                    self.put_to_queue_(bars_list[-1][0], bars_list[-1][1], OrderPosition.SELL)

## Sklearn classifier (combined)
class RawClassification(RawRegression):
    def __init__(self, bars, events, clf, processor, reoptimize_days):
        RawRegression.__init__(self, bars, events, clf, processor, reoptimize_days)
    
    def calculate_signals(self, event):
        if event.type == "MARKET":
            for s in self.symbol_list:
                bars_list = self.bars.get_latest_bars(s, N=self.processor.get_shift())

                close_price = bars_list[-1][5]
                if len(bars_list) != self.processor.get_shift() or \
                    bars_list[-1][-1] == 0.0:
                    return

                ## this is slow and should be optimized.
                preds = self._prepare_flow_data(bars_list, s)
                if preds is None:
                    return
                diff = (preds[-1] - close_price)/ close_price
                if diff > 0.05:
                    self.put_to_queue_(bars_list[-1][0], bars_list[-1][1], OrderPosition.BUY)
                elif diff < -0.04:
                    self.put_to_queue_(bars_list[-1][0], bars_list[-1][1], OrderPosition.SELL)
