from abc import ABCMeta, abstractmethod
import logging

import numpy as np
import pandas as pd
from backtest.event import SignalEvent
from backtest.strategy.naive import Strategy
from backtest.utilities.enums import OrderPosition
from sklearn.preprocessing import LabelEncoder


class BuyDips(Strategy):
    def __init__(self, bars, events, short_time, long_time, consecutive=2) -> None:
        self.bars = bars
        self.events = events
        self.st = short_time
        self.lt = long_time
        self.consecutive = consecutive

    def calculate_position(self, bars) -> OrderPosition: 
        lt_corr = np.corrcoef(np.arange(1,bars.shape[0]+1), bars[:, 5].astype('float32'))
        st_corr = np.corrcoef(np.arange(1, self.st+1), bars[-self.st:, 5].astype('float32'))
        # price_pct_change = np.diff(bars[:, 5].astype('float32')) / bars[1:, 5].astype('float32') * 100
        ## dips
        if lt_corr[0][1] > 0.40 and np.percentile(bars[-self.st*2:, 5], 25) > bars[-1,5]:
            return OrderPosition.BUY
        # elif lt_corr[0][1] < -0.40 and np.percentile(bars[-self.st*2:, 5], 90) < bars[-1,5]:
        #     return OrderPosition.SELL
        # #momentum
        elif lt_corr[0][1] > 0.20 and st_corr[0][1] > 0.75 and np.percentile(bars[:, 5], 25) > bars[-1,5]:
            return OrderPosition.BUY
        ## currenly selling at dips
        elif st_corr[0][1] < -0.75 and np.percentile(bars[:, 5], 75) < bars[-1,5]:
            return OrderPosition.SELL
     
    def calculate_signals(self, event):
        if event.type == 'MARKET':
            # corr_diff = dict((sym, None) for lt_corr[0][1] < -0.2540and sym in self.bars.symbol_lis)
            for sym in self.bars.symbol_list:
                bars = self.bars.get_latest_bars(sym, N=self.lt+self.consecutive)
                if len(bars['datetime']) != self.lt+self.consecutive:
                    continue
                psignals = [self.calculate_position(bars['close'][:-i]) for i in range(1,self.consecutive)]
                if all(elem == psignals[-1] for elem in psignals) and psignals[-1] is not None:
                    self.put_to_queue_(bars['symbol'], bars['datetime'][-1], psignals[-1], bars['close'][-1])

class StatisticalStrategy(Strategy):
    def __init__(self, bars, events, model, processor):
        super().__init__(bars, events)
        self.events = events
        self.columns = ["symbol", "datetime", "Open", "High", "Low", "close", "Volume"]
        self.model = model
        self.bars = bars
        self.symbols_list = self.bars.symbol_list
        self.processor = processor
    
    def _prepare_flow_data(self, bars):
        temp_df = pd.DataFrame(bars, columns = self.columns)
        temp_df = temp_df.drop(["symbol", "date"], axis=1)
        return self.model.predict(self.processor.preprocess_X(temp_df))
    

## Sklearn regressor (combined)
class RawRegression(StatisticalStrategy):
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
        ## data is dict
        for sym in self.bars.symbol_list:
            bars = self.bars.get_latest_bars(sym, N=self.reoptimize_days)
            data = pd.DataFrame.from_dict(bars)
            data.set_index('datetime', inplace=True)
            data.drop('symbol', axis=1, inplace=True)
            X, y = self.processor.process_data(data)
            if self.model[sym] is None and not (X["close"] == 0).any():
                try:
                    self.model[sym] = self.reg()
                    self.model[sym].fit(X,y)
                except Exception as e:
                    logging.exception("Fit does not work.")
                    logging.exception(e)
                    logging.error(X,y)
            
    def _prepare_flow_data(self, bars, sym):
        temp_df = pd.DataFrame.from_dict(bars)
        temp_df = temp_df.drop(["symbol", "datetime"], axis=1)
        return None if self.model[sym] is None else self.model[sym].predict(
            self.processor._transform_X(temp_df))

    def calculate_signals(self, event):
        if event.type == "MARKET":
            for s in self.bars.symbol_list:
                bars = self.bars.get_latest_bars(s, N=self.processor.get_shift())
                if len(bars['datetime']) < self.processor.get_shift() or \
                    bars['close'][-1] == 0.0:
                    return

                ## this is slow and should be optimized.
                preds = self._prepare_flow_data(bars, s)
                if preds is None:
                    return
                if preds[-1] > 0.09:
                    self.put_to_queue_(bars['symbol'], bars['datetime'][-1], 
                                    OrderPosition.BUY,  bars['close'][-1])
                elif preds[-1] < -0.09:
                    self.put_to_queue_(bars['symbol'], bars['datetime'][-1], 
                                    OrderPosition.SELL, bars['close'][-1])


## Sklearn classifier (combined)
class RawClassification(RawRegression):
    def __init__(self, bars, events, clf, processor, reoptimize_days):
        RawRegression.__init__(self, bars, events, clf, processor, reoptimize_days)
    
    def calculate_signals(self, event):
        if event.type == "MARKET":
            for s in self.bars.symbol_list:
                bars = self.bars.get_latest_bars(s, N=self.processor.get_shift())

                close_price = bars['close'][-1]
                if len(bars['datetime']) != self.processor.get_shift() or \
                    bars['close'][-1] == 0.0:
                    return

                ## this is slow and should be optimized.
                preds = self._prepare_flow_data(bars, s)
                if preds is None:
                    return
                diff = (preds[-1] - close_price)/ close_price
                if diff > 0.05:
                    self.put_to_queue_(bars['symbol'], bars['datetime'][-1], 
                                    OrderPosition.BUY,  bars['close'][-1])
                elif diff < -0.04:
                    self.put_to_queue_(bars['symbol'], bars['datetime'][-1], 
                                    OrderPosition.SELL,  bars['close'][-1])
