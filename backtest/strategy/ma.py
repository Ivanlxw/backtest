import numpy as np
import pandas as pd
import talib

from backtest.event import SignalEvent
from backtest.strategy.naive import Strategy

def ExponentialMA(close_prices, timeperiod):
    EMA = talib.EMA(pd.Series(close_prices), timeperiod) 
    return EMA.ema_indicator().values

class SimpleCrossStrategy(Strategy):
    '''
    Buy/Sell when it crosses the smoothed line (SMA, etc.)
    '''
    def __init__(self, bars, events, timeperiod:int, ma_type):    
        self.bars = bars  ## datahandler
        self.symbol_list = self.bars.symbol_list
        self.events = events
        self.timeperiod= timeperiod
        self.ma_type = ma_type

    def _get_MA(self, bars):
        close_prices = np.array([tup[5] for tup in bars])
        return self.ma_type(close_prices, self.timeperiod)

    def calculate_signals(self, event):
        if event.type == "MARKET":
            for s in self.symbol_list:
                bars = self.bars.get_latest_bars(s, N=(self.timeperiod+1)) ## list of tuples
                if len(bars) != self.timeperiod+1:
                    break
                TAs = self._get_MA(bars)
                if bars[-2][5] > TAs[-2] and bars[-1][5] < TAs[-1]:
                    signal = SignalEvent(bars[-1][0], bars[-1][1], 'SHORT')
                    self.events.put(signal)
                elif bars[-2][5] < TAs[-2] and bars[-1][5] > TAs[-1]:
                    signal =  SignalEvent(bars[-1][0], bars[-1][1], 'LONG')
                    self.events.put(signal)

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
            if cross_type == "sma":
                self.cross_function = self._get_SMA
            elif cross_type == "ema":
                self.cross_function = self._get_EMA
            for s in self.symbol_list:
                bars = self.bars.get_latest_bars(s, N=(self.timeperiod*2)) ## list of tuples
                if len(bars) < self.timeperiod*2:
                    break
                TAs = self.cross_function(bars)
                sd_TA = self._get_sd(TAs)
                boundary = sd_TA*self.sd_multiplier

                if self.exit == "cross":
                    if bars[-2][5] > TAs[-2] and bars[-1][5] < TAs[-1] or \
                        bars[-2][5] < TAs[-2] and bars[-1][5] > TAs[-1]:
                        signal = SignalEvent(bars[-1][0], bars[-1][1], 'REVERSE')
                        self.events.put(signal)
                    elif bars[-1][5] > (TAs[-1] + boundary):
                        signal = SignalEvent(bars[-1][0], bars[-1][1], 'SHORT')
                        self.events.put(signal)
                    elif bars[-1][5] < (TAs[-1] - boundary):
                        signal = SignalEvent(bars[-1][0], bars[-1][1], 'LONG')
                        self.events.put(signal)

                elif self.exit == "bb":
                    if bars[-2][5] > TAs[-2] and bars[-1][5] < TAs[-1]:
                        signal = SignalEvent(bars[-1][0], bars[-1][1], 'SHORT')
                        self.events.put(signal)
                    elif bars[-2][5] < TAs[-2] and bars[-1][5] > TAs[-1]:
                        signal =  SignalEvent(bars[-1][0], bars[-1][1], 'LONG')
                        self.events.put(signal)
                    elif bars[-1][5] > (TAs[-1] + boundary) or bars[-1][5] < (TAs[-1] - boundary):
                        signal = SignalEvent(bars[-1][0], bars[-1][1], 'REVERSE')
                        self.events.put(signal)