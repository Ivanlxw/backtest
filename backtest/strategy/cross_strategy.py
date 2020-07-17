import numpy as np
import pandas as pd
import talib

from event import SignalEvent
from naive import Strategy

supported_types=["sma", "ema"]

class SimpleCrossStrategy(Strategy):
    '''
    Buy/Sell when it crosses the smoothed line (SMA, etc.)
    '''
    def __init__(self, bars, events, cross_type:str, timeperiod:int):
        if cross_type.lower() not in supported_types:
            raise Exception(f"TA strategy not available. Choose from the following:\n{supported_types}") 
    
        self.bars = bars  ## datahandler
        self.symbol_list = self.bars.symbol_list
        self.events = events
        self.timeperiod= timeperiod
        self.cross_type = cross_type

        if cross_type == "sma":
            self.cross_function = self._get_SMA
        elif cross_type == "ema":
            self.cross_function = self._get_EMA
    
    def _get_SMA(self, bars):
        close_prices = np.array([tup[5] for tup in bars])
        return talib.SMA(close_prices, self.timeperiod)

    def _get_EMA(self, bars):
        close_prices = np.array([tup[5] for tup in bars])
        EMA = talib.EMA(pd.Series(close_prices), self.timeperiod) 
        # return talib.EMA(close_prices, self.timeperiod)
        return EMA.ema_indicator().values

    def calculate_signals(self, event):
        if event.type == "MARKET":
            for s in self.symbol_list:
                bars = self.bars.get_latest_bars(s, N=(self.timeperiod+1)) ## list of tuples
                if len(bars) == self.timeperiod+1:
                    TAs = self.cross_function(bars)
                    if bars[-2][5] > TAs[-2] and bars[-1][5] < TAs[-1]:
                        signal = SignalEvent(bars[-1][0], bars[-1][1], 'SHORT')
                        self.events.put(signal)
                        print(f"short stock: {bars[-1][0]}")
                    elif bars[-2][5] < TAs[-2] and bars[-1][5] > TAs[-1]:
                        signal =  SignalEvent(bars[-1][0], bars[-1][1], 'LONG')
                        self.events.put(signal)
                        print(f"long stock: {bars[-1][0]}")

class MeanReversionTA(SimpleCrossStrategy):
    '''
    Strategy is based on the assumption that prices will always revert to the smoothed line.
    Will buy/sell when it crosses the smoothed line and EXIT when it reaches beyond 
    the confidence interval, calculated with sd - and Vice-versa works as well
    which method to use is denoted in exit - "cross" or "bb"
    '''
    def __init__(self, bars, events, cross_type:str, timeperiod:int, sd:float=2, exit="cross"):
        if cross_type.lower() not in supported_types:
            raise Exception(f"TA strategy not available. Choose from the following:\n{supported_types}") 
        super().__init__(bars, events, cross_type, timeperiod)
        self.sd_multiplier = sd
        self.exit = exit
  
    def _get_sd(self, ta_array):
        return np.std(ta_array[-self.timeperiod:])
    
    def calculate_signals(self, event):
        '''
        LONG and SHORT criterion:
        - same as simple cross strategy, just that if price are outside the bands, 
        employ mean reversion.
        '''
        if event.type == "MARKET":
            for idx,s in enumerate(self.symbol_list):
                bars = self.bars.get_latest_bars(s, N=(self.timeperiod*2)) ## list of tuples
                if len(bars) >= self.timeperiod*2:
                    TAs = self.cross_function(bars)
                    sd_TA = self._get_sd(TAs)

                    if self.exit == "cross":
                        if bars[-2][5] > TAs[-2] and bars[-1][5] < TAs[-1]:
                            signal = SignalEvent(bars[-1][0], bars[-1][1], 'REVERSE')
                            self.events.put(signal)
                        elif bars[-2][5] < TAs[-2] and bars[-1][5] > TAs[-1]:
                            signal =  SignalEvent(bars[-1][0], bars[-1][1], 'REVERSE')
                        elif bars[-1][5] > (TAs[-1] + sd_TA*self.sd_multiplier):
                            signal = SignalEvent(bars[-1][0], bars[-1][1], 'SHORT')
                            self.events.put(signal)
                        elif bars[-1][5] < (TAs[-1] + sd_TA*self.sd_multiplier):
                            signal = SignalEvent(bars[-1][0], bars[-1][1], 'LONG')
                            self.events.put(signal)

                    elif self.exit == "bb":
                        if bars[-2][5] > TAs[-2] and bars[-1][5] < TAs[-1]:
                            signal = SignalEvent(bars[-1][0], bars[-1][1], 'SHORT')
                            self.events.put(signal)
                        elif bars[-2][5] < TAs[-2] and bars[-1][5] > TAs[-1]:
                            signal =  SignalEvent(bars[-1][0], bars[-1][1], 'LONG')
                            self.events.put(signal)
                        elif bars[-1][5] > (TAs[-1] + sd_TA*self.sd_multiplier):
                            signal = SignalEvent(bars[-1][0], bars[-1][1], 'REVERSE')
                            self.events.put(signal)
                        elif bars[-1][5] < (TAs[-1] + sd_TA*self.sd_multiplier):
                            signal = SignalEvent(bars[-1][0], bars[-1][1], 'REVERSE')
                            self.events.put(signal)