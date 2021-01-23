from abc import ABCMeta, abstractmethod
from backtest.utilities.enums import OrderPosition

from numpy.lib.stride_tricks import broadcast_arrays
from backtest.event import SignalEvent
from backtest.strategy.naive import Strategy
import numpy as np

class FundamentalStrategy(Strategy):
    __metaclass__ = ABCMeta
    
    def __init__(self, bars, events) -> None:
        super().__init__()
        self.bars = bars
        self.events = events
    
    @abstractmethod
    def calculate_signals(self, event):
        raise NotImplementedError("Please use a subclass of FundamentalStrategy")

class FundamentalFScoreStrategy(FundamentalStrategy):
    def __init__(self, bars, events) -> None:
        super().__init__(bars, events)
        self.scores = {sym:[] for sym in self.bars.symbol_list}
        assert self.bars.fundamental_data is not None

    def _get_quarter_and_year(self, bars_list):
        date = bars_list[1]
        return (date.year, int((date.month -1)/3) + 1)
    
    def _calc_score(self, bars_list: list):
        year,qtr = self._get_quarter_and_year(bars_list[-1])
        fundamental_data = list(filter(
                lambda x: x['year'] == year and \
                x['quarter'] == qtr,
                self.bars.fundamental_data[bars_list[-1][0]]
        ))
        if len(fundamental_data) > 0:
            close_price = bars_list[-1][5]
            ma = np.average(np.array(bars_list)[:,5])
            # print(fundamental_data)
            metrics = dict((x['dataCode'],x['value']) for x in fundamental_data[0]['statementData']['overview'])
            return metrics['piotroskiFScore'] + (ma-close_price)/ma

    def calculate_signals(self, event):
        if event.type != 'MARKET':
            return
        for sym in self.bars.symbol_list:
            bars_list = self.bars.get_latest_bars(sym, 30)
            if len(bars_list) < 30:
                continue
            score = self._calc_score(bars_list)
            if not score:
                return
            self.scores[sym].append(score)

            if len(self.scores[sym]) < 30:
                continue 
            if score > np.percentile(self.scores[sym][-min(50, len(self.scores)):], 95):
                self.events.put(SignalEvent(bars_list[-1][0], bars_list[-1][1], OrderPosition.BUY))
            elif score < np.percentile(self.scores[sym][-min(50, len(self.scores)):], 5):
                self.events.put(SignalEvent(bars_list[-1][0], bars_list[-1][1], OrderPosition.SELL))
            