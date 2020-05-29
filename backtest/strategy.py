"""
Strategy object take market data as input and produce trading signal events as output
"""

# strategy.py

import datetime
import numpy as np
import pandas as pd
import queue
import talib

from abc import ABCMeta, abstractmethod

from event import SignalEvent

class Strategy(object):
    """
    Strategy is an abstract base class providing an interface for
    all subsequent (inherited) strategy handling objects.

    This is designed to work both with historic and live data as
    the Strategy object is agnostic to the data source, since it
    obtains the bar tuples from a queue object.
    """
    __metaclass__ = ABCMeta
    def calculate_signals(self):
        raise NotImplementedError("Should implement calculate_signals()")

class BuyAndHoldStrategy(Strategy):
    """
    LONG all the symbols as soon as a bar is received. Next exit its position

    A benchmark to compare other strategies
    """

    def __init__(self, bars, events):
        """
        Args:
        bars - DataHandler object that provides bar info
        events - event queue object
        """

        self.bars = bars  ## datahandler
        self.symbol_list = self.bars.symbol_list
        self.events = events 

        self.bought = self._calculate_initial_bought()
    
    def _calculate_initial_bought(self):
        bought = {}
        for s in self.symbol_list:
            bought[s] = False
        return bought
    
    def calculate_signals(self, event):
        if event.type == "MARKET":
            for s in self.symbol_list:
                bars = self.bars.get_latest_bars(s, N=1)

                if bars is not None and bars != []: ## there's an entry
                    if self.bought[s] == False:
                        signal = SignalEvent(bars[0][0], bars[0][1], 'LONG')
                        self.events.put(signal)
                        self.bought[s] = True

supported_types=["sma", "ema"]
class SimpleCrossStrategy(Strategy):
    def __init__(self, bars, events, timeperiod, cross_type:str):
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
        return talib.EMA(close_prices, self.timeperiod)

    def calculate_signals(self, event):
        if event.type == "MARKET":
            for s in self.symbol_list:
                bars = self.bars.get_latest_bars(s, N=(self.timeperiod+1)) ## list of tuples
                if len(bars) == self.timeperiod+1:
                    SMAs = self.cross_function(bars)
                    if bars[-2][5] > SMAs[-2] and bars[-1][5] < SMAs[-1]:
                        ## break downwards, SHORT
                        # print("Ytd end price" ,SMAs[-2], bars[-2][5])
                        # print("Today end price" ,SMAs[-1], bars[-1][5])
                        signal = SignalEvent(bars[-1][0], bars[-1][1], 'SHORT')
                        self.events.put(signal)
                    elif bars[-2][5] < SMAs[-2] and bars[-1][5] > SMAs[-1]:
                        ## break upwards, LONG
                        signal =  SignalEvent(bars[-1][0], bars[-1][1], 'LONG')
                        self.events.put(signal)

