from abc import ABC, abstractmethod
from trading.event import SignalEvent
from trading.strategy.naive import Strategy
import numpy as np
from trading.utilities.enum import OrderPosition


class FundamentalFilter(Strategy):
    def __init__(self, bars) -> None:
        super().__init__()
        self.bars = bars
        self.scores = []

    def _get_quarter_and_year(self, bars_list):
        date = bars_list[1]
        return (date.year, int((date.month - 1)/3) + 1)

    def _calc_score(self, bars_list: list, fundamental_data: list):
        close_price = bars_list[-1][5]
        ma = np.average(np.array(bars_list)[:, 5])
        metrics = dict((x['dataCode'], x['value'])
                       for x in fundamental_data[0]['statementData']['overview'])
        return metrics['piotroskiFScore'] + (metrics['roe'] + metrics['roa']) * (ma-close_price)/ma

    def _calculate_signal(self, ticker) -> SignalEvent:
        bars_list = self.bars.get_latest_bars(ticker, 30)
        year, qtr = self._get_quarter_and_year(bars_list[-1])
        fundamental_data = list(filter(
            lambda x: x['year'] == year and
            x['quarter'] == qtr,
            self.bars.fundamental_data[ticker]
        ))
        assert len(fundamental_data) <= 1
        if not fundamental_data:
            return
        score = self._calc_score(bars_list, fundamental_data)
        self.scores.append(score)
        if len(self.scores) > 200 and \
                score > np.percentile(self.scores[-min(1000, len(self.scores)):], 92):
            return SignalEvent(ticker, bars_list["datetime"][-1], OrderPosition.SELL, bars_list["close"][-1])
