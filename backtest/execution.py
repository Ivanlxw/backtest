from backtest.utilities.enums import OrderType
import datetime
import time

from abc import ABC, abstractmethod
from backtest.event import FillEvent

# from ib.ext.Contract import Contract
# from ib.ext.Order import Order
# from ib.opt import ibConnection, message

class ExecutionHandler(ABC):
    @abstractmethod
    def execute_order(self, event):
        """
        Takes an OrderEvent and execute it, producing
        FillEvent that gets places into Events Queue
        """
        raise NotImplementedError("Should implement execute_order()")

"""
assumes all orders are filled at the current market price for all quantities
TODO: 
- include slippage and market impact
- make use of the "current" market data value to obtain a realistic fill cost.
"""

## arbitrary. Might be used to route orders to a broker in future.
class SimulatedExecutionHandler(ExecutionHandler):
    def __init__(self, bars, events):
        self.bars = bars
        self.events = events
    
    def calculate_commission(self,quantity, fill_cost):
        return 0

    def execute_order(self, event):
        if event.type == 'ORDER':
            if event.order_type == OrderType.LIMIT:
                price_data = self.bars.get_latest_bars(event.symbol, 1)
                if event.trade_price > price_data[0][3] or event.trade_price < price_data[0][4]:
                    return
            fill_event = FillEvent(datetime.datetime.utcnow(), event.symbol,
                "ARCA", event.trade_price, event.quantity, event.direction, None, self.calculate_commission)
            self.events.put(fill_event)

class IBExecutionHandler(ExecutionHandler):
    def __init__(self,events, order_routing="SMART", currency="USD"):
        self.events = events
        self.order_routing = order_routing
        self.currency = currency
        self.fill_dict = {}

        self.tws_conn = self.create_tws_connection()
        self.order_id = self.create_initial_order_id()
        self.register_handlers()

    def _error_handler(self, msg):
        # Handle open order orderId processing
        if msg.typeName == "openOrder" and \
            msg.orderId == self.order_id and \
            not self.fill_dict.has_key(msg.orderId):
            self.create_fill_dict_entry(msg)
        # Handle Fills
        elif msg.typeName == "orderStatus" and \
            msg.status == "Filled" and \
            self.fill_dict[msg.orderId]["filled"] == False:
            self.create_fill(msg)      
        print("Server Response: {}, {}\n".format(msg.typeName, msg))
    
    def create_tws_connection(self):
        """
        Connect to the Trader Workstation (TWS) running on the
        usual port of 7496, with a clientId of 10.
        The clientId is chosen by us and we will need 
        separate IDs for both the execution connection and
        market data connection, if the latter is used elsewhere.
        """
        tws_conn = ibConnection()
        tws_conn.connect()
        return tws_conn
    
    def create_initial_order_id(self):
        """
        Creates the initial order ID used for Interactive
        Brokers to keep track of submitted orders.

        Can always reset the current API order ID via: 
        Trader Workstation > Global Configuration > API Settings panel:
        """
        # will use "1" as the default for now.
        return 1

    def register_handlers(self):
        """
        Register the error and server reply  message handling functions.
        """
        # Assign the error handling function defined above
        # to the TWS connection
        self.tws_conn.register(self._error_handler, 'Error')

        # Assign all of the server reply messages to the
        # reply_handler function defined above
        self.tws_conn.registerAll(self._reply_handler)

    
    ## create a Contract instance and then pair it with an Order instance, 
    ## which will be sent to the IB API

    def create_contract(self, symbol, sec_type, exch, prim_exch, curr):
        contract = Contract()
        contract.m_symbol = symbol
        contract.m_secType = sec_type
        contract.m_exchange = exch
        contract.m_primaryExch = prim_exch
        contract.m_currency = curr

        return contract

    def create_order(self, order_type, quantity, action):
        """
            order_type - MARKET, LIMIT for Market or Limit orders
            quantity - Integral number of assets to order
            action - 'BUY' or 'SELL'
        """
        order = Order()
        order.m_orderType = order_type
        order.m_totalQuantity = quantity
        order.m_action = action

        return order

    def create_fill_dict_entry(self, msg):
        """
        needed for the event-driven behaviour of the IB
        server message behaviour.
        """
        self.fill_dict[msg.orderId] = {
            "symbol": msg.contract.m_symbol,
            "exchange": msg.contract.m_exchange,
            "direction": msg.order.m_action,
            "filled": False
        }
    
    def create_fill(self, msg):
        fd = self.fill_dict[msg.orderId]
        symbol = fd["symbol"]
        exchange = fd["exchange"]
        filled = msg.filled
        direction = fd["direction"]
        fill_cost = msg.avgFillPrice

        fill = FillEvent(datetime.datetime.utcnow(), symbol,
            exchange, filled, direction, fill_cost)
        
        self.fill_dict[msg.orderId]["filled"] = True

        self.events.put(fill)
    
    def execute_order(self, event):
        if event.type == "ORDER":
            asset = event.symbol
            asset_type = "STK"
            order_type = event.order_type
            quantity = event.quantity
            direction = event.direction

            # Create the Interactive Brokers contract via the 
            # passed Order event
            ib_contract = self.create_contract(
                asset, asset_type, self.order_routing,
                self.order_routing, self.currency
            )

            # Create the Interactive Brokers order via the 
            # passed Order event
            ib_order = self.create_order(
                order_type, quantity, direction
            )

            # Use the connection to the send the order to IB
            self.tws_conn.placeOrder(
                self.order_id, ib_contract, ib_order
            )

            time.sleep(1)
            self.order_id += 1
    
    def calculate_ib_commission(quantity, fill_cost):
        full_cost = 1.3
        if quantity <= 500: 
            full_cost = max(1.3, 0.013 * quantity)
        else:
            full_cost = max(1.3, 0.008 * quantity)
        full_cost = min(full_cost, 0.005 * quantity * fill_cost)

        return full_cost