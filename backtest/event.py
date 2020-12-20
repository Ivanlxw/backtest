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

    def __init__(self, symbol, datetime, signal_type,):
        ## SignalEvent('GOOG', timestamp, 'LONG')    # timestamp can be a string or the big numbers
        self.type = 'SIGNAL'
        self.symbol = symbol
        self.datetime = datetime
        assert signal_type in ('SHORT', 'LONG', 'EXIT')
        self.signal_type = signal_type

class OrderEvent(Event):
    """
    assesses signalevents in the wider context of the portfolio, 
    in terms of risk and position sizing. 
    This ultimately leads to OrderEvents that will be sent to an ExecutionHandler.
    """
    def __init__(self, symbol, order_type, quantity, direction):
        """ Params
        order_type - 'MKT' or 'LMT' for Market or Limit
        quantity - non-nevgative integer
        direction - 'BUY' or 'SELL' for long or short
        """

        self.type = 'ORDER'
        self.symbol = symbol
        self.order_type = order_type
        self.quantity = quantity
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
    ## FillEvent(timeindex, 'GOOG', 'S&P500', 20, 'BUY', )
    def __init__(self, timeindex, symbol, exchange, trade_price, quantity, direction, fill_cost, \
        calculate_commission, commission=None):
        """
        Parameters:
        timeindex - The bar-resolution when the order was filled.
        symbol - The instrument which was filled.
        exchange - The exchange where the order was filled.
        quantity - The filled quantity.
        direction - The direction of fill ('BUY' or 'SELL')
        fill_cost - The holdings value in dollars.
        commission - An optional commission sent from IB.
        """
        self.type= 'FILL'
        self.timeindex = timeindex
        self.symbol = symbol
        self.exchange = exchange
        self.trade_price = trade_price
        self.quantity = quantity
        self.direction = direction
        self.fill_cost = fill_cost

        self.calculate_commission = calculate_commission

        if commission is None:
            self.commission = self.calculate_commission(quantity, fill_cost)
        else:
            self.commission = commission

class OptimizeEvent(Event):
    """
    Gives signal for strategy to optimize statistical models
    """
    def __init__(self):
        self.type = "OPTIMIZE"