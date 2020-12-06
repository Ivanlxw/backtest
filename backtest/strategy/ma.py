from time import time
from matplotlib.pyplot import bar
import numpy as np
import pandas as pd
import talib

from backtest.event import SignalEvent
from backtest.strategy.naive import Strategy

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

    def _get_MA(self, bars, timeperiod):
        close_prices = np.array([tup[5] for tup in bars])
        return self.ma_type(close_prices, timeperiod)
    
    def _break_up(self, bars: list, TAs: list) -> bool:
        return bars[-2][5] < TAs[-2] and bars[-1][5] > TAs[-1]
    
    def _break_down(self, bars: list, TAs: list) -> bool:
        return bars[-2][5] > TAs[-2] and bars[-1][5] < TAs[-1]

    def calculate_signals(self, event):
        if event.type != "MARKET":
            return
        for s in self.symbol_list:
            bars = self.bars.get_latest_bars(s, N=(self.timeperiod+3)) ## list of tuples
            if len(bars) != self.timeperiod+3:
                continue
            TAs = self._get_MA(bars, self.timeperiod)
            if bars[-2][5] > TAs[-2] and bars[-1][5] < TAs[-1]:
                signal = SignalEvent(bars[-1][0], bars[-1][1], 'SHORT')
                self.events.put(signal)
            elif bars[-2][5] < TAs[-2] and bars[-1][5] > TAs[-1]:
                signal =  SignalEvent(bars[-1][0], bars[-1][1], 'LONG')
                self.events.put(signal)

class DoubleMAStrategy(SimpleCrossStrategy):
    def __init__(self, bars, events, timeperiods , ma_type):    
        super().__init__(bars, events, 0, ma_type)
        self.shorter = min(timeperiods)
        self.longer = max(timeperiods) 
    
    def calculate_signals(self, event):
        '''
        Earn from the gap between shorter and longer ma
        '''
        if event.type != "MARKET":
            return
        for s in self.symbol_list:
            bars = self.bars.get_latest_bars(s, N=(self.longer+3)) ## list of tuples
            if len(bars) < self.longer+3:
                continue
            short_ma = self._get_MA(bars, self.shorter)
            long_ma = self._get_MA(bars, self.longer)

            if short_ma[-1] < long_ma[-1]:
                signal = SignalEvent(bars[-1][0], bars[-1][1], 'SHORT')
                self.events.put(signal) 
            elif short_ma[-1] > long_ma[-1]:
                signal = SignalEvent(bars[-1][0], bars[-1][1], 'LONG')
                self.events.put(signal) 
            # elif self._break_up(bars, short_ma) or self._break_down(bars, long_ma):
            #     signal = SignalEvent(bars[-1][0], bars[-1][1], 'EXIT')
            #     self.events.put(signal) 

class MeanReversionTA(SimpleCrossStrategy):
    '''
    Strategy is based on the assumption that prices will always revert to the smoothed line.
    Will buy/sell when it crosses the smoothed line and EXIT when it reaches beyond 
    the confidence interval, calculated with sd - and Vice-versa works as well
    which method to use is denoted in exit - "cross" or "bb"
    '''
    def __init__(self, bars, events, timeperiod:int, ma_type, sd:float=2, exit: bool=True):
        super().__init__(bars, events, timeperiod, ma_type)
        self.sd_multiplier = sd
        self.exit = exit
  
    def _get_sd(self, ta_array):
        return np.std(ta_array[-self.timeperiod:])
    
    def _exit_ma_cross(self, bars, TAs, boundary):
        if self._break_down(bars, TAs) or self._break_up(bars, TAs):
            signal = SignalEvent(bars[-1][0], bars[-1][1], 'EXIT')
            self.events.put(signal)            
        
        if (bars[-1][5] < (TAs[-1] + boundary) and bars[-2][5] > (TAs[-2] + boundary)):
            signal = SignalEvent(bars[-1][0], bars[-1][1], 'SHORT')
            self.events.put(signal)
        elif (bars[-1][5] > (TAs[-1] - boundary) and bars[-2][5] < (TAs[-2] - boundary)):
            signal = SignalEvent(bars[-1][0], bars[-1][1], 'LONG')
            self.events.put(signal)
    
    def calculate_signals(self, event):
        '''
        LONG and SHORT criterion:
        - same as simple cross strategy, just that if price are outside the bands, 
        employ mean reversion.
        '''
        if event.type != "MARKET":
            return

        for s in self.symbol_list:
            bars = self.bars.get_latest_bars(s, N=(self.timeperiod*2)) ## list of tuples
            if len(bars) < self.timeperiod+3:
                continue
            TAs = self._get_MA(bars, self.timeperiod)
            sd_TA = self._get_sd(TAs)
            boundary = sd_TA*self.sd_multiplier

            if self.exit:
                self._exit_ma_cross(bars, TAs, boundary)
                continue

            if self._break_down(bars, TAs) or \
                (bars[-1][5] < (TAs[-1] + boundary) and bars[-2][5] > (TAs[-2] + boundary)):
                signal = SignalEvent(bars[-1][0], bars[-1][1], 'SHORT')
                self.events.put(signal)
            elif self._break_up(bars, TAs) or \
                (bars[-1][5] > (TAs[-1] - boundary) and bars[-2][5] < (TAs[-2] - boundary)):
                signal = SignalEvent(bars[-1][0], bars[-1][1], 'LONG')
                self.events.put(signal)
    