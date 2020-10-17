import os, sys
sys.path.append((os.path.dirname(os.path.abspath(__file__))))  ## 2 dirs above

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

from strategy.naive import Strategy
from event import SignalEvent

class SkStrategy():
    def __init__(self, bars, events, reg, processor):
        self.X = None
        self.Y = None
        self.events = events
        self.columns = None
        self.reg = reg
        self.bars = bars
        self.symbol_list = self.bars.symbol_list
        self.processor = processor
        self.scaler = StandardScaler()

        self._set_X_and_Y()
        self._train()
    
    def _set_X_and_Y(self,):
        self.X, self.Y = self.processor.get_processed_data()
        self.columns = ["symbol", "date"] + list(self.X.columns)

    def _train(self,):
        self.reg.fit(self.X, self.Y)
    
    def _prepare_flow_data(self, bars):
        temp_df = pd.DataFrame(bars, columns=self.columns)
        temp_df = temp_df.drop(["symbol", "date"], axis=1)
        temp_df = pd.DataFrame(self.scaler.fit_transform(temp_df), columns = self.columns[2:])
        return self.reg.predict(self.processor.transform_X(temp_df))
    

## Sklearn regressor (combined)
## data is combined and standardized.
class SKRStrategy(SkStrategy, Strategy):
    def __init__(self, bars, events, reg, processor):
        SkStrategy.__init__(self, bars, events, reg, processor)

    def calculate_signals(self, event):
        if event.type == "MARKET":
            for s in self.symbol_list:
                bars_list = self.bars.get_latest_bars(s, N=self.processor.get_shift())
                if len(bars_list) != self.processor.get_shift() or \
                    bars_list[-1][-1] == 0.0:
                    return

                #standardize values
                ## this is slow and should be optimized.
                preds = self._prepare_flow_data(bars_list)

                if preds[-1] > 0.5:
                    signal = SignalEvent(bars_list[-1][0], bars_list[-1][1], 'LONG')
                    self.events.put(signal)
                elif preds[-1] < -0.4:
                    signal = SignalEvent(bars_list[-1][0], bars_list[-1][1], 'SHORT')
                    self.events.put(signal)

## Sklearn classifier (combined)
## data is combined and standardized.
class SKCStrategy(SkStrategy, Strategy):
    def __init__(self, bars, events, clf, processor):
        SkStrategy.__init__(self, bars, events, clf, processor)
    
    def _set_X_and_Y(self,):
        from sklearn.preprocessing import LabelBinarizer, LabelEncoder
        self.label = LabelEncoder() 
        self.X, self.Y = self.processor.get_processed_data()
        self.columns = ["symbol", "date"] + list(self.X.columns)
        self.Y = self.label.fit_transform(self.Y)

    def calculate_signals(self, event):
        if event.type == "MARKET":
            for s in self.symbol_list:
                bars = self.bars.get_latest_bars(s, N=self.processor.get_shift())
                if len(bars) != self.processor.get_shift():
                    return

                #standardize values
                ## this is slow and should be optimized.
                preds = self._prepare_flow_data(bars)

                if preds[-1] == 2:
                    signal = SignalEvent(bars[-1][0], bars[-1][1], 'LONG')
                    self.events.put(signal)
                elif preds[-1] == 0:
                    signal = SignalEvent(bars[-1][0], bars[-1][1], 'SHORT')
                    self.events.put(signal)
