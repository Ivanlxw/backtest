from abc import ABCMeta, abstractmethod
from backtest.utilities.enums import OrderPosition, OrderType
from backtest.event import OrderEvent
from math import fabs

class PortfolioStrategy(metaclass=ABCMeta):
    def __init__(self, bars, current_positions, all_holdings, order_events, events, order_type):
        self.current_positions = current_positions
        self.all_holdings = all_holdings
        self.bars = bars
        self.order_events = order_events
        self.events = events
        self.order_type = order_type
    
    @abstractmethod
    def generate_order(self, signal, size, current_positions):
        """
        The check for rebalancing portfolio
        """
        raise NotImplementedError("Should implement generate_order()")

    @abstractmethod
    def filter_order_to_send(self, order_event):
        """
        Updates portfolio based on rebalancing criteria
        """
        raise NotImplementedError("Should implement filter_order_to_send(). If not required, just pass")


class DefaultOrder(PortfolioStrategy):
    def __init__(self, bars, current_positions, all_holdings, order_events, events, order_type):
        super().__init__(bars, current_positions, all_holdings, order_events, events, order_type)

    def generate_order(self, signal, size) -> None:
        """
        takes a signal to long or short an asset and then sends an order 
        of size=size of such an asset
        """
        order = None
        symbol = signal.symbol
        direction = signal.signal_type

        cur_quantity = self.current_positions[symbol]

        if direction == OrderPosition.EXIT:
            if cur_quantity > 0:
                order = OrderEvent(symbol, self.order_type, cur_quantity, OrderPosition.SELL)
            else:
                order = OrderEvent(symbol, self.order_type, -cur_quantity, OrderPosition.BUY)            
        elif direction == OrderPosition.BUY:
            if cur_quantity < 0:
                order = OrderEvent(symbol, self.order_type, size-cur_quantity, direction)
            else:
                order = OrderEvent(symbol, self.order_type, size, direction)
        elif direction == OrderPosition.SELL:
            if cur_quantity > 0:
                order = OrderEvent(symbol, self.order_type, size+cur_quantity, direction)
            else:
                order = OrderEvent(symbol, self.order_type, size, direction)
        if order is not None:
            self.filter_order_to_send(order)

    def filter_order_to_send(self, order_event:OrderEvent):
        mkt_price = self.bars.get_latest_bars(order_event.symbol)[0][5]
        order_value = fabs(order_event.quantity * mkt_price)
        if order_event.direction == OrderPosition.BUY and self.all_holdings[-1]["cash"] > order_value or \
            self.all_holdings[-1]['total'] > order_value and order_event.direction == OrderPosition.SELL:
            order_event.trade_price = mkt_price

            if order_event.order_type == OrderType.LIMIT:
                self.order_events.put(order_event)
            else:
                self.events.put(order_event)

class LongOnly(PortfolioStrategy):
    def __init__(self, bars, current_positions, all_holdings, order_events, events, order_type=OrderType.MARKET):
        super().__init__(bars, current_positions, all_holdings, order_events, events, order_type)
        self.order_type = order_type
    
    def generate_order(self, signal, size):
        """
        takes a signal, short=exit 
        and then sends an order of size=size of such an asset
        """
        order = None
        symbol = signal.symbol
        direction = signal.signal_type
        
        if direction == OrderPosition.BUY:
            order = OrderEvent(symbol, self.order_type, size, direction)
        elif (direction == OrderPosition.SELL or direction == OrderPosition.EXIT) and self.current_positions[symbol] > 0:
            order = OrderEvent(symbol, self.order_type, self.current_positions[symbol], OrderPosition.SELL)
        if order is not None:
            self.filter_order_to_send(order)

    def filter_order_to_send(self, order_event: OrderEvent):
        mkt_price = self.bars.get_latest_bars(order_event.symbol)[0][5]
        order_value = fabs(order_event.quantity * mkt_price)
        if order_event.direction == OrderPosition.BUY and self.all_holdings[-1]["cash"] < order_value:
            return
        order_event.trade_price = mkt_price
        if order_event.order_type == OrderType.LIMIT:
            self.order_events.put(order_event)
        else:
            self.events.put(order_event)
