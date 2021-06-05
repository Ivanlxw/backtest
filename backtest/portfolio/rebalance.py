from abc import ABCMeta, abstractmethod
from backtest.utilities.enums import OrderPosition
from backtest.event import SignalEvent

class Rebalance(metaclass=ABCMeta):                
    @abstractmethod
    def need_rebalance(self, all_holdings):
        """
        The check for rebalancing portfolio
        """
        raise NotImplementedError("Should implement need_rebalance()")

    @abstractmethod
    def rebalance(self, stock_list, all_holdings):
        """
        Updates portfolio based on rebalancing criteria
        """
        raise NotImplementedError("Should implement rebalance(). If not required, just pass")

class NoRebalance(Rebalance):
    ''' No variables initialized as need_balance returns false'''
    def __init__(self) -> None:
        return
    
    def need_rebalance(self, all_holdings):
        return False

    def rebalance(self, stock_list, all_holdings) -> None:
        return


class BaseRebalance(Rebalance):
    ''' EXIT for all positions every year '''
    def __init__(self, events) -> None:
        self.events = events

    def need_rebalance(self, all_holdings):
        return all_holdings[-1]['datetime'].year != all_holdings[-2]['datetime'].year

    def rebalance(self, stock_list, all_holdings) -> None:
        if self.need_rebalance(all_holdings):
            for symbol in stock_list:
                ## only 1 will go through if there is position
                self.events.put(SignalEvent(symbol, all_holdings[-1]['datetime'], OrderPosition.EXIT_LONG))
                self.events.put(SignalEvent(symbol, all_holdings[-1]['datetime'], OrderPosition.EXIT_SHORT))

class SellLongLosers(BaseRebalance):
    def __init__(self, events) -> None:
        super().__init__()

    def need_rebalance(self, all_holdings):
        return all_holdings[-1]['datetime'].year != all_holdings[-2]['datetime'].year

    def rebalance(self, stock_list, all_holdings) -> None:
        if self.need_rebalance(all_holdings):
            for symbol in stock_list:
                ## TODO: Check for losing stocks
                self.events.put(SignalEvent(symbol, all_holdings[-1]['datetime'], OrderPosition.EXIT_LONG))
                self.events.put(SignalEvent(symbol, all_holdings[-1]['datetime'], OrderPosition.EXIT_SHORT))