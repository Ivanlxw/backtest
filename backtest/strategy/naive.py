from trading.event import SignalEvent
from trading.strategy.naive import Strategy
from trading.utilities.enum import OrderPosition


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
        super().__init__(bars, events)
        self._initialize_bought_status()

    def _initialize_bought_status(self,):
        self.bought = {}
        for s in self.bars.symbol_list:
            self.bought[s] = False

    def _calculate_signal(self, symbol) -> SignalEvent:
        if not self.bought[symbol]:
            bars = self.bars.get_latest_bars(symbol, N=1)
            if bars is not None and bars != []:  # there's an entry
                self.bought[symbol] = True
                return SignalEvent(symbol, bars['datetime'][-1], OrderPosition.BUY, bars['close'][-1])
