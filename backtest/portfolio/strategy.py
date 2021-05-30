from abc import ABCMeta, abstractmethod
from backtest.data.dataHandler import DataHandler

from backtest.event import OrderEvent, SignalEvent
from backtest.utilities.enums import OrderPosition, OrderType


class PortfolioStrategy(metaclass=ABCMeta):
    def __init__(self, bar:DataHandler, current_positions, order_type:OrderType) -> None:
        self.bar = bar
        self.current_holdings = current_positions
        self.order_type = order_type

    @abstractmethod
    def _filter_order_to_send(self, signal:SignalEvent) -> OrderEvent:
        """
        Updates portfolio based on rebalancing criteria
        """
        raise NotImplementedError("Should implement filter_order_to_send(order_event). If not required, just pass")


class DefaultOrder(PortfolioStrategy):    
    def _filter_order_to_send(self, signal: SignalEvent) -> OrderEvent:
        """
        takes a signal to long or short an asset and then sends an order 
        of signal.quantity=signal.quantity of such an asset
        """
        assert signal.quantity is not None
        order = None
        symbol = signal.symbol
        direction = signal.signal_type
        latest_snapshot = self.bar.get_latest_bars(signal.symbol)

        cur_quantity = self.current_holdings[symbol]

        if direction == OrderPosition.EXIT_LONG:
            if cur_quantity > 0:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], cur_quantity, OrderPosition.SELL, signal.price)
        elif direction == OrderPosition.EXIT_SHORT:
            if cur_quantity < 0:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], -cur_quantity, OrderPosition.BUY, signal.price)            
        elif direction == OrderPosition.BUY and cur_quantity <= 0:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], signal.quantity-cur_quantity, direction, signal.price)
        elif direction == OrderPosition.SELL and cur_quantity >= 0:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], signal.quantity+cur_quantity, direction, signal.price)
        if order is not None:
            order.signal_price = signal.price
        return order

class ProgressiveOrder(PortfolioStrategy):
    def _filter_order_to_send(self, signal:SignalEvent) -> OrderEvent:
        """
        takes a signal to long or short an asset and then sends an order 
        of signal.quantity=signal.quantity of such an asset
        """
        assert signal.quantity is not None
        order = None
        symbol = signal.symbol
        direction = signal.signal_type
        latest_snapshot = self.bar.get_latest_bars(signal.symbol)

        cur_quantity = self.current_holdings[symbol]

        if direction == OrderPosition.EXIT_LONG:
            if cur_quantity > 0:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], cur_quantity, OrderPosition.SELL, signal.price)
        elif direction == OrderPosition.EXIT_SHORT:
            if cur_quantity < 0:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], -cur_quantity, OrderPosition.BUY, signal.price)            
        elif direction == OrderPosition.BUY:
            if cur_quantity < 0:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], signal.quantity-cur_quantity, direction, signal.price)
            else:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], signal.quantity, direction, signal.price)
        elif direction == OrderPosition.SELL:
            if cur_quantity > 0:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], signal.quantity+cur_quantity, direction, signal.price)
            else:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], signal.quantity, direction, signal.price)
        if order is not None:
            order.signal_price = signal.price
        return order

class LongOnly(PortfolioStrategy):
    def _filter_order_to_send(self, signal:SignalEvent):
        """
        takes a signal, short=exit 
        and then sends an order of signal.quantity=signal.quantity of such an asset
        """
        assert signal.quantity is not None
        order = None
        symbol = signal.symbol
        direction = signal.signal_type
        latest_snapshot = self.bar.get_latest_bars(signal.symbol)

        if direction == OrderPosition.BUY:
            order = OrderEvent(symbol, latest_snapshot['datetime'][-1], signal.quantity, 
            direction, signal.price)
        elif (direction == OrderPosition.SELL or direction == OrderPosition.EXIT_LONG) \
            and self.current_holdings[symbol] > 0:
            order = OrderEvent(symbol, latest_snapshot['datetime'][-1], 
                self.current_holdings[symbol], OrderPosition.SELL, signal.price)
        if order is not None:
            order.signal_price = signal.price
        return order
