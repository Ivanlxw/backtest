""" [DEPRECIATED]  
TODO:
Remove this file
"""
from abc import ABCMeta, abstractmethod
from math import fabs
from datetime import timedelta

from backtest.event import OrderEvent
from backtest.utilities.enums import OrderPosition


class PortfolioStrategy(metaclass=ABCMeta):
  @classmethod
  def generate_order(cls, signal, latest_snapshot, current_holdings, holdings_value:dict, size, expires:int) -> OrderEvent:
    order = cls._filter_order_to_send(latest_snapshot, signal, current_holdings, size)
    return cls._enough_credit(order, latest_snapshot, current_holdings, holdings_value, size, expires)

  @classmethod
  def _enough_credit(cls, order, latest_snapshot, current_holdings, holdings_value, size, expires:int) -> OrderEvent:
        if order is None:
            return 
        mkt_price = latest_snapshot['close'][-1]
        order_value = fabs(size * mkt_price)
        if (order.direction == OrderPosition.BUY and current_holdings["cash"] > order_value) or \
            (holdings_value["total"] > order_value and order.direction == OrderPosition.SELL):
            order.trade_value = mkt_price
            order.expires = latest_snapshot['datetime'][-1] + timedelta(days=expires)
            return order
  @abstractmethod
  def _filter_order_to_send(signal, latest_snapshot):
      """
      Updates portfolio based on rebalancing criteria
      """
      raise NotImplementedError("Should implement filter_order_to_send(order_event). If not required, just pass")


class DefaultOrder(PortfolioStrategy):    
    @classmethod
    def _filter_order_to_send(cls, latest_snapshot, signal, current_holdings, size) -> OrderEvent:
        """
        takes a signal to long or short an asset and then sends an order 
        of size=size of such an asset
        """
        order = None
        symbol = signal.symbol
        direction = signal.signal_type

        cur_quantity = current_holdings[symbol]

        if direction == OrderPosition.EXIT:
            if cur_quantity > 0:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], cur_quantity, OrderPosition.SELL, signal.price)
            elif cur_quantity < 0:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], -cur_quantity, OrderPosition.BUY, signal.price)            
        elif direction == OrderPosition.BUY:
            if cur_quantity <= 0:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], size-cur_quantity, direction, signal.price)
            else:
                return
        elif direction == OrderPosition.SELL:
            if cur_quantity >= 0:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], size+cur_quantity, direction, signal.price)
            else:
                return
        return order

class ProgressiveOrder(PortfolioStrategy):
    @classmethod
    def _filter_order_to_send(cls, latest_snapshot, signal, current_holdings, size) -> OrderEvent:
        """
        takes a signal to long or short an asset and then sends an order 
        of size=size of such an asset
        """
        order = None
        symbol = signal.symbol
        direction = signal.signal_type

        cur_quantity = current_holdings[symbol]

        if direction == OrderPosition.EXIT:
            if cur_quantity > 0:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], cur_quantity, OrderPosition.SELL, signal.price)
            elif cur_quantity < 0:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], -cur_quantity, OrderPosition.BUY, signal.price)            
        elif direction == OrderPosition.BUY:
            if cur_quantity < 0:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], size-cur_quantity, direction, signal.price)
            else:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], size, direction, signal.price)
        elif direction == OrderPosition.SELL:
            if cur_quantity > 0:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], size+cur_quantity, direction, signal.price)
            else:
                order = OrderEvent(symbol, latest_snapshot['datetime'][-1], size, direction, signal.price)
        return order

class LongOnly(PortfolioStrategy):
    @classmethod
    def _filter_order_to_send(cls, latest_snapshot, signal, current_holdings, size):
        """
        takes a signal, short=exit 
        and then sends an order of size=size of such an asset
        """
        order = None
        symbol = signal.symbol
        direction = signal.signal_type
        
        if direction == OrderPosition.BUY:
            order = OrderEvent(symbol, latest_snapshot['datetime'][-1], size, 
            direction, signal.price)
        elif (direction == OrderPosition.SELL or direction == OrderPosition.EXIT) \
            and current_holdings[symbol] > 0:
            order = OrderEvent(symbol, latest_snapshot['datetime'][-1], 
                current_holdings[symbol], OrderPosition.SELL, signal.price)
        return order
