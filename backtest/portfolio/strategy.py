from abc import ABCMeta, abstractmethod
from backtest.utilities.enums import OrderPosition, OrderType
from backtest.event import OrderEvent
from math import fabs

class PortfolioStrategy(metaclass=ABCMeta):
  @classmethod
  def generate_order(cls, signal, latest_snapshot, current_holdings, holdings_value:dict, size=None) -> OrderEvent:
    order = cls._filter_order_to_send(signal, latest_snapshot, current_holdings, holdings_value, size)
    return cls._enough_credit(order, latest_snapshot, current_holdings, holdings_value, size)

  @classmethod
  def _enough_credit(cls, order, latest_snapshot, current_holdings, holdings_value, size) -> OrderEvent:
        if order is None:
            return 
        mkt_price = latest_snapshot[5]
        order_value = fabs(size * mkt_price)
        if order.direction == OrderPosition.BUY and current_holdings["cash"] > order_value or \
            holdings_value["total"] > order_value and order.direction == OrderPosition.SELL:
            order.trade_price = mkt_price
            return order
  @abstractmethod
  def _filter_order_to_send(signal, latest_snapshot):
      """
      Updates portfolio based on rebalancing criteria
      """
      raise NotImplementedError("Should implement filter_order_to_send(order_event). If not required, just pass")


class DefaultOrder(PortfolioStrategy):    
    @classmethod
    def _filter_order_to_send(cls, signal, latest_snapshot, current_holdings, holdings_value, size) -> OrderEvent:
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
                order = OrderEvent(symbol, cur_quantity, OrderPosition.SELL)
            elif cur_quantity < 0:
                order = OrderEvent(symbol, -cur_quantity, OrderPosition.BUY)            
        elif direction == OrderPosition.BUY:
            if cur_quantity < 0:
                order = OrderEvent(symbol, size-cur_quantity, direction)
            else:
                order = OrderEvent(symbol, size, direction)
        elif direction == OrderPosition.SELL:
            if cur_quantity > 0:
                order = OrderEvent(symbol, size+cur_quantity, direction)
            else:
                order = OrderEvent(symbol, size, direction)
        return order
class LongOnly(PortfolioStrategy):
    @classmethod
    def _filter_order_to_send(cls, signal, snapshot, current_holdings, holdings_value, size):
        """
        takes a signal, short=exit 
        and then sends an order of size=size of such an asset
        """
        order = None
        symbol = signal.symbol
        direction = signal.signal_type
        
        if direction == OrderPosition.BUY:
            order = OrderEvent(symbol, size, direction)
        elif (direction == OrderPosition.SELL or direction == OrderPosition.EXIT) and current_holdings[symbol] > 0:
            order = OrderEvent(symbol, current_holdings[symbol], OrderPosition.SELL)
        return order