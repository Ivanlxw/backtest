import datetime
import os
import threading
import time
from abc import ABC, abstractmethod
from pandas.core import api
import requests
import pandas as pd

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper
import alpaca_trade_api

from backtest.event import FillEvent, OrderEvent
from backtest.utilities.enums import OrderPosition, OrderType


class Broker(ABC):
    @abstractmethod
    def execute_order(self, event):
        """
        Takes an OrderEvent and execute it, producing
        FillEvent that gets places into Events Queue
        """
        raise NotImplementedError("Should implement execute_order()")

    @abstractmethod
    def calculate_commission(self,):
        """
        Takes an OrderEvent and calculates commission based 
        on the details of the order it, add as argument for 
        FillEvent
        """
        raise NotImplementedError("Implement calculate_commission()")

"""
assumes all orders are filled at the current market price for all quantities
TODO: 
- include slippage and market impact
- make use of the "current" market data value to obtain a realistic fill cost.
"""

## arbitrary. Might be used to route orders to a broker in future.
class SimulatedBroker(Broker):
    def __init__(self, bars, events, order_queue):
        self.bars = bars
        self.events = events
        self.order_queue = order_queue
    
    def calculate_commission(self,quantity=None, fill_cost=None) -> float:
        return 0.0

    def execute_order(self, event):
        if event.type == 'ORDER':
            if event.order_type == OrderType.LIMIT:
                price_data = self.bars.get_latest_bars(event.symbol, 1)[0]
                ## check for expiry
                '''                
                print("Price data:", price_data)
                print("OrderEvent: ", {
                    "Symbol": event.symbol, 
                    "Price": event.signal_price, 
                    "Direction": event.direction,
                    "Expires": event.expires
                })
                '''
                if price_data[1] >= event.expires:
                    return

                if event.signal_price > price_data[3] and event.direction == OrderPosition.BUY:
                    self.order_queue.put(event)
                    return
                elif event.signal_price < price_data[4] and event.direction == OrderPosition.SELL:
                    self.order_queue.put(event)
                    return
            fill_event = FillEvent(event, self.calculate_commission())
            self.events.put(fill_event)

class IBBroker(Broker, EWrapper, EClient):
    def __init__(self, events):
        EClient.__init__(self, self)
        self.events = events
        self.fill_dict = {}
        self.hist_data = []

        self.connect('127.0.0.1', 7497, 123)
        self.tws_conn = self.create_tws_connection()
        self.reqMarketDataType(3) ## DELAYED
        self.order_id = self.create_initial_order_id()
        # self.register_handlers()

    def create_tws_connection(self) -> None:
        """
        Connect to the Trader Workstation (TWS) running on the
        usual port of 7496, with a clientId of 10.
        The clientId is chosen by us and we will need 
        separate IDs for both the broker connection and
        market data connection, if the latter is used elsewhere.
        - Should run in a new thread
        """
        def run_loop():
            self.run()
        
        api_thread = threading.Thread(target=run_loop, daemon=True)
        api_thread.start()
        time.sleep(1)  ## to allow connection to server

    def create_initial_order_id(self):
        """
        Creates the initial order ID used for Interactive
        Brokers to keep track of submitted orders.

        Can always reset the current API order ID via: 
        Trader Workstation > Global Configuration > API Settings panel:
        """
        # will use "1" as the default for now.
        return 1

    ## create a Contract instance and then pair it with an Order instance, 
    ## which will be sent to the IB API
    def create_contract(self, symbol, sec_type, exchange="SMART", currency="USD"):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = sec_type
        contract.exchange = exchange
        contract.currency = currency
        return contract

    ## Overwrite EClient.historicalData
    def historicalData(self, reqId, bar):
        print(f'Time: {bar.date} Close: {bar.close}')
        self.hist_data.append([bar.date, bar.close])
        # if eurusd_contract.symbol in my_td_broker.hist_data.keys():
        #     my_td_broker.hist_data[eurusd_contract.symbol].append()

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


    def create_order(self, order_type, quantity, action):
        """
            order_type - MARKET, LIMIT for Market or Limit orders
            quantity - Integral number of assets to order
            action - 'BUY' or 'SELL'
        """
        order = OrderEvent()
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
    
    def calculate_commission(self, quantity, fill_cost):
        full_cost = 1.3
        if quantity <= 500: 
            full_cost = max(1.3, 0.013 * quantity)
        else:
            full_cost = max(1.3, 0.008 * quantity)
        full_cost = min(full_cost, 0.005 * quantity * fill_cost)

        return full_cost

## not working. Seems like a developer account needs to be created with TDA
class TDABroker(Broker):
    def __init__(self) -> None:
        super(TDABroker, self).__init__()

    # def get_token(self, grant_type, ):
    #     params = {
    #         "grant_type": grant_type,
    #         "client_id": self.client_id
    #     }
    #     res = requests.post(f"https://api.tdameritrade.com/v1/oauth2/token", params=params)
    #     print(res)
    #     print(res.json())
    
    def get_quote(self, symbol):
        return requests.get(f"https://api.tdameritrade.com/v1/marketdata/{symbol}/quotes", params={
            "apikey": "OSFN56WSXXTRMUS6BIUOFFMGGKSNKDHN",
            "Authorization": "Bearer mrb7zZyTbQVGAfZA6BP5c/S14dS62docTeuG3NF+3YjoeT61o8SNZ0EWRcWpwIjzretF6vNiRMi8ZVtHPBATKYfwyRSeNSMykwxIIHN3jVLYpv8jhqI/wBNruBcvPqjksyd5m75LKm5UGUQ3NWKU3oP7zXN/bQDsohwVq4BrYc9luYJLk5NNNgpcrHMB1FVOQg6sksyBY67CXuuv4W7Kxxg9YQKWgaoHuQF6jYC9+HLE8iUMNBY0CAaHoM+OzF7qo7GC/UENRJXYANcdhM86YLNlERmOUCVISMj2CNIOiutR/BVTrD8r2yV0bvwzwInfVXp615EJcGn8KVQBA5a1/D3VVKGSrCnfIJmMHQtMaD3i2Z12P/8Kfx9OEPuHM+ylTap7YVhb2f84DtI/GhBorFfMs3ZnNmzY4OVhHMQr/YecXwk15PewMOiK01+4ZhPqe5auwRBRLfb7ujoxx1+syIgYiDzJ/fu0n0UJ+AlngV8HkbjnkQ0r4IcAVMXnXhut6U+UWZ+9PFafwIn7XWf4fE1Nf41+/rr0QcgPQnkp1r5aGv9DS100MQuG4LYrgoVi/JHHvlZee7GMbvqzVlJYRJF19WEfQeAy0/hx39Ghm3U8jd6wD5oRFCeG8/qxF3voFUGOQqSQG5jJkCJjJo3FbDwtAVWdUjTzrqAF2sLfIiCVGF7WN8lyv2xpGyHegu0sauMxOP18CDn292jGUYMw3Hi9gNr1M8g8AJqG7FtZYF/nBbMscYuhKlJ9IlP+SWone9730Owmrc3ltRyUOrh78yPsv/WmOU9qMO6bhY8WLcs7JQ62GvHdXMX9lAhRsP1NGcckyH0Slun/ODMJ8+hkLXk98ByzU+eIl6+/lik+vhMMz7SlHh5y5mMhn/prXA2sIC4OmaIzHMyE8lU1nuZamQmMeeo1bK38ZnBrVxC9sBk9FS09rD+woQn21K527WVy5A5Gd05XKiAitOmG0lof+txK8tyf5aWOqhK2lQSO6Fu2CgimISNjnwjr4bOqzlYjlGeyevyEqfD7CGxyNSTbUP624mQmM2kBPcv7rpQcpcTlaTj2m25CxlFk0mUlkP8gJ+cIR3wiF207QleZa8dQ1p9QQjxrjjk6JKC/uxWgnj2u4oH6koQxgQ==212FD3x19z9sWBHDJACbC00B75E"      
        }).json()
    
    def calculate_commission(self):
        return 0
    
    def execute_order(self, event):
        return None

class AlpacaBroker(Broker):
    def __init__(self,):
        self.base_url = "https://paper-api.alpaca.markets"
        self.data_url = "https://data.alpaca.markets/v2" 
        self.api = alpaca_trade_api.REST(
            os.environ["alpaca_key_id"], 
            os.environ["alpaca_secret_key"], 
            self.base_url, api_version="v2"
        )

    def _alpaca_endpoint(self, url, args: str):
        return requests.get(url+args, headers={
            "APCA-API-KEY-ID": os.environ["alpaca_key_id"],
            "APCA-API-SECRET-KEY": os.environ["alpaca_secret_key"]
        }).json()

    """ ORDERS """
    def get_current_orders(self):
        return self.api.list_orders()

    def execute_order(self, event: OrderEvent):
        side = 'buy' if OrderPosition.BUY else 'sell'
        if event.order_type == OrderType.LIMIT:
            return self.api.submit_order(
                symbol=event.symbol,
                qty=event.quantity, side=side, 
                type='limit', time_in_force='day',
                limit_price=OrderEvent.signal_price)
        else:
           return self.api.submit_order(
                symbol=event.symbol,
                qty=event.quantity, side=side, 
                type='market', time_in_force='day')

    def calculate_commission(self):
        return 0
    
    """ PORTFOLIO RELATED """
    def get_positions(self):
        return self.api.list_positions()

    def get_historical_bars(self, ticker, timeframe, start, end, limit:int = None) -> pd.DataFrame:
        assert timeframe in ['1Min', '5Min', '15Min', 'day', '1D']
        if limit is not None:
            return self.api.get_barset(ticker, timeframe, start=start, end=end, limit=limit).df
        return self.api.get_barset(ticker, timeframe, start=start, end=end).df
   
    def get_quote(self, ticker):
        return self.api.get_last_quote(ticker)

