from backtest.utilities.enums import OrderPosition, OrderType


class Event(object):
    """
    Event is base class providing an interface for all subsequent 
    (inherited) events, that will trigger further events in the 
    trading infrastructure.   
    """
    pass

class MarketEvent(Event):
    """
    Handles devent of receiving new market update with corresponding bars

    Triggered when the outer while loop begins a new "heartbeat". It occurs when the 
    DataHandler object receives a new update of market data for any symbols which are 
    currently being tracked. 
    It is used to trigger the Strategy object generating new trading signals. 
    The event object simply contains an identification that it is a market event
    """

    def __init__(self):
        self.type = "MARKET"


class SignalEvent(Event):
    """
    Handles the event of sending a Signal from a Strategy object.
    This is received by a Portfolio object and acted upon.

    Utilises market data. SignalEvent contains a ticker symbol, a timestamp 
    for when it was generated and a direction (long or short). 
    The SignalEvents are utilised by the Portfolio object as advice for how to trade.
    """

    def __init__(self, symbol, datetime, signal_type: OrderPosition):
        ## SignalEvent('GOOG', timestamp, OrderPosition.LONG)    # timestamp can be a string or the big numbers
        self.type = 'SIGNAL'
        self.symbol = symbol
        self.datetime = datetime
        self.signal_type = signal_type

class OrderEvent(Event):
    """
    assesses signalevents in the wider context of the portfolio, 
    in terms of risk and position sizing. 
    This ultimately leads to OrderEvents that will be sent to an ExecutionHandler.
    """
    def __init__(self, symbol, quantity, direction: OrderPosition):
        """ Params
        order_type - MARKET or LIMIT for Market or Limit
        quantity - non-nevgative integer
        direction - BUY or SELL for long or short
        """

        self.type = 'ORDER'
        self.symbol = symbol
        self.order_type = None
        self.quantity = quantity
        assert (direction == OrderPosition.BUY or direction == OrderPosition.SELL)
        self.direction = direction
        self.trade_price = None
    
    def print_order(self,):
        print("Order: Symbol={}, Type={}, Trade Price = {}, Quantity={}, Direction={}".format(self.symbol, \
            self.order_type, self.trade_price, self.quantity, self.direction))

class FillEvent(Event):
    """
    Encapsulates the notion of a Filled Order, as returned
    from a brokerage. Stores the quantity of an instrument
    actually filled and at what price. In addition, stores
    the commission of the trade from the brokerage.

    When an ExecutionHandler receives an OrderEvent it must transact the order. 
    Once an order has been transacted it generates a FillEvent
    """
    ## FillEvent(order_event, calculate_commission())
    def __init__(self, order_event, commission):
        """
        Parameters:
        timeindex - The bar-resolution when the order was filled.
        symbol - The instrument which was filled.
        exchange - The exchange where the order was filled.
        quantity - The filled quantity.
        direction - The direction of fill (BUY or SELL)
        fill_cost - The holdings value in dollars.
        commission - An optional commission sent from IB.
        """
        self.type= 'FILL'
        self.order_event = order_event
        self.commission = commission

class OptimizeEvent(Event):
    """
    Gives signal for strategy to optimize statistical models
    """
    def __init__(self):
        self.type = "OPTIMIZE"