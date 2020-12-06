"""
Strategy object take market data as input and produce trading signal events as output
"""

# strategy.py

import datetime
import numpy as np
import pandas as pd
import queue

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
    
    def calculate_signals(self, event):
        if event.type == "MARKET":
            for s in self.symbol_list:
                bars = self.bars.get_latest_bars(s, N=1)
                if bars is not None and bars != []: ## there's an entry
                    signal = SignalEvent(bars[0][0], bars[0][1], 'LONG')
                    self.events.put(signal)