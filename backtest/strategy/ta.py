from time import time
import numpy as np
import pandas as pd
import talib
from backtest.event import SignalEvent
from backtest.strategy.naive import Strategy
from backtest.utilities.enums import OrderPosition
from matplotlib.pyplot import bar

class SimpleCrossStrategy(Strategy):
    '''
    Buy/Sell when it crosses the smoothed line (SMA, etc.)
    '''
    def __init__(self, bars, events, timeperiod:int, ma_type):    
        self.bars = bars  ## barshandler
        self.symbol_list = self.bars.symbol_list
        self.events = events
        self.timeperiod= timeperiod
        self.ma_type = ma_type

    def _get_MA(self, bars, timeperiod):
        close_prices = np.array([tup for tup in bars])
        return self.ma_type(close_prices, timeperiod)
    
    def _break_up(self, bars: list, TAs: list) -> bool:
        return bars[-2] < TAs[-2] and bars[-1] > TAs[-1]
    
    def _break_down(self, bars: list, TAs: list) -> bool:
        return bars[-2] > TAs[-2] and bars[-1] < TAs[-1]

    def calculate_signals(self, event):
        if event.type != "MARKET":
            return
        for s in self.symbol_list:
            bars = self.bars.get_latest_bars(s, N=(self.timeperiod+3)) ## list of tuples
            if len(bars) != self.timeperiod+3:
                continue
            TAs = self._get_MA(bars, self.timeperiod)
            if bars['close'][-2] > TAs[-2] and bars['close'][-1] < TAs[-1]:
                self.put_to_queue_(bars['symbol'], bars['datetime'][-1], OrderPosition.SELL, bars['close'][-1])
            elif bars[-2] < TAs[-2] and bars[-1] > TAs[-1]:
                self.put_to_queue_(bars['symbol'], bars['datetime'][-1], OrderPosition.BUY,  bars['close'][-1])

class DoubleMAStrategy(SimpleCrossStrategy):
    def __init__(self, bars, events, timeperiods , ma_type):    
        super().__init__(bars, events, 0, ma_type)
        if len(timeperiods) != 2:
            raise Exception("Time periods have to be a list/tuple of length 2")
        self.shorter = min(timeperiods)
        self.longer = max(timeperiods) 
    
    def _cross(self, short_ma_list, long_ma_list)-> int: 
        '''
        returns 1, 0 or -1 corresponding cross up, nil and cross down
        '''
        if short_ma_list[-1] > long_ma_list[-1] and short_ma_list[-2] < long_ma_list[-2]:
            return 1
        elif short_ma_list[-1] < long_ma_list[-1] and short_ma_list[-2] > long_ma_list[-2]:
            return -1
        return 0
    
    def calculate_signals(self, event):
        '''
        Earn from the gap between shorter and longer ma
        '''
        if event.type != "MARKET":
            return
        for s in self.symbol_list:
            bars = self.bars.get_latest_bars(s, N=(self.longer+3)) ## list of tuples
            if len(bars['datetime']) < self.longer+3:
                continue
            short_ma = self._get_MA(bars['close'], self.shorter)
            long_ma = self._get_MA(bars['close'], self.longer)
            sig = self._cross(short_ma, long_ma)
            if sig == -1:
                self.put_to_queue_(bars['symbol'], bars['datetime'][-1], OrderPosition.SELL, bars['close'][-1])
            elif sig == 1:
                self.put_to_queue_(bars['symbol'], bars['datetime'][-1], OrderPosition.BUY, bars['close'][-1])

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
    
    def _exit_ma_cross(self, bars, TAs, boundary):
        if self._break_down(bars['close'], TAs):
            self.put_to_queue_(bars['symbol'], bars['datetime'][-1], OrderPosition.EXIT_SHORT, bars['close'][-1])          
        elif self._break_up(bars['close'], TAs):
            self.put_to_queue_(bars['symbol'], bars['datetime'][-1], OrderPosition.EXIT_SHORT, bars['close'][-1])          

        if (bars['close'][-1] < (TAs[-1] + boundary) and bars['close'][-2] > (TAs[-2] + boundary)):
            self.put_to_queue_(bars['symbol'], bars['datetime'][-1], OrderPosition.SELL, bars['close'][-1])
        elif (bars['close'][-1] > (TAs[-1] - boundary) and bars['close'][-2] < (TAs[-2] - boundary)):
            self.put_to_queue_(bars['symbol'], bars['datetime'][-1], OrderPosition.BUY, bars['close'][-1])
    
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
            if len(bars['datetime']) < self.timeperiod+3:
                continue
            if 'close' in bars:
                close_prices = bars['close']
                TAs = self._get_MA(close_prices, self.timeperiod)
                sd_TA = np.std(TAs[-self.timeperiod:])
                boundary = sd_TA*self.sd_multiplier

                if self.exit:
                    self._exit_ma_cross(bars, TAs, boundary)
                    continue

                if self._break_down(close_prices, TAs) or \
                    (close_prices[-1] < (TAs[-1] + boundary) and close_prices[-2] > (TAs[-2] + boundary)):
                    self.put_to_queue_(bars['symbol'], bars['datetime'][-1], OrderPosition.SELL, bars['close'][-1])
                elif self._break_up(close_prices, TAs) or \
                    (close_prices[-1] > (TAs[-1] - boundary) and close_prices[-2] < (TAs[-2] - boundary)):
                    self.put_to_queue_(bars['symbol'], bars['datetime'][-1], OrderPosition.BUY, bars['close'][-1])

class CustomRSI(Strategy):
    def __init__(self, bars, events, rsi_period, long_period):
        self.bars = bars
        self.events = events
        self.rsi_period = rsi_period
        self.period = long_period

    def calculate_signals(self, event):
        for sym in self.bars.symbol_list:
            bars = self.bars.get_latest_bars(sym, self.period+3)
            if len(bars['datetime']) < self.period+3:
                continue
            rsi_values = talib.RSI(np.array(bars['close']), self.rsi_period)
            if rsi_values[-1] > 40 and all(rsi < 40 for rsi in rsi_values[-3:-1]) \
                and np.corrcoef(np.arange(1, self.rsi_period+1), bars['close'][-self.rsi_period:])[1][0] > 0.30:
                self.put_to_queue_(bars['symbol'], bars['datetime'][-1], OrderPosition.BUY, bars['close'][-1])
            elif rsi_values[-1] < 50 and all(rsi > 50 for rsi in rsi_values[-3:-1]) \
                and np.corrcoef(np.arange(1,self.rsi_period+1), bars['close'][-self.rsi_period:])[1][0] < -0.75:
                self.put_to_queue_(bars['symbol'], bars['datetime'][-1], OrderPosition.SELL, bars['close'][-1])
