from backtest.utilities.utils import log_message
import datetime
import os
import threading
import time
import logging
from abc import ABC, abstractmethod
import requests
from math import fabs
import pandas as pd
import json

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper
import alpaca_trade_api

from trading.event import FillEvent, OrderEvent
from trading.utilities.enum import OrderPosition, OrderType


class Broker(ABC):
    @abstractmethod
    def _filter_execute_order(self, event: OrderEvent) -> bool:
        """
        Takes an OrderEvent and checks if it can be executed
        """
        raise NotImplementedError("Should implement _filter_execute_order()")

    def execute_order(self, event) -> bool:
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
TODO: 
- include slippage and market impact
- make use of the "current" market data value to obtain a realistic fill cost.
"""

# arbitrary. Might be used to route orders to a broker in future.


class SimulatedBroker(Broker):
    def __init__(self, bars, port, events, order_queue):
        self.bars = bars
        self.port = port
        self.events = events
        self.order_queue = order_queue

    def calculate_commission(self, quantity=None, fill_cost=None) -> float:
        return 0.0

    def _enough_credits(self, order: OrderEvent, latest_snapshot) -> bool:
        if order is None:
            return False
        mkt_price = latest_snapshot["close"][-1]
        order_value = fabs(order.quantity * mkt_price)
        if (
            order.direction == OrderPosition.BUY
            and self.port.current_holdings["cash"] > order_value
        ) or (
            self.port.all_holdings[-1]["total"] > order_value
            and order.direction == OrderPosition.SELL
        ):
            return True
        return False

    def _filter_execute_order(self, order_event: OrderEvent) -> bool:
        latest_snapshot = self.bars.get_latest_bars(order_event.symbol)
        if self._enough_credits(order_event, latest_snapshot):
            if order_event.order_type == OrderType.LIMIT:
                """                
                print("Price data:", price_data_dict)
                print("OrderEvent: ", {
                    "Symbol": event.symbol, 
                    "Price": event.signal_price, 
                    "Direction": event.direction,
                    "Expires": event.expires
                })
                """
                # check for expiry
                if latest_snapshot["datetime"][-1] > order_event.expires:
                    return False

                if (
                    order_event.signal_price > latest_snapshot["high"][-1]
                    and order_event.direction == OrderPosition.BUY
                ) or (
                    order_event.signal_price < latest_snapshot["low"][-1]
                    and order_event.direction == OrderPosition.SELL
                ):
                    return False
            return True
        return False

    def execute_order(self, event: OrderEvent) -> bool:
        if event.type == "ORDER" and self._filter_execute_order(event):
            close_price = self.bars.get_latest_bars(event.symbol)["close"][
                -1
            ]  # close price
            event.trade_price = close_price
            if event.order_type == OrderType.LIMIT and not event.processed:
                event.processed = True
                event.date += datetime.timedelta(days=1)
                self.order_queue.put(event)
                return False
            else:
                fill_event = FillEvent(event, self.calculate_commission())
                self.events.put(fill_event)
                return True
        return False


class IBBroker(Broker, EWrapper, EClient):
    def __init__(self, events):
        EClient.__init__(self, self)
        self.events = events
        self.fill_dict = {}
        self.hist_data = []

        self.connect("127.0.0.1", 7497, 123)
        self.tws_conn = self.create_tws_connection()
        self.reqMarketDataType(3)  # DELAYED
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
        time.sleep(1)  # to allow connection to server

    def create_initial_order_id(self):
        """
        Creates the initial order ID used for Interactive
        Brokers to keep track of submitted orders.

        Can always reset the current API order ID via: 
        Trader Workstation > Global Configuration > API Settings panel:
        """
        # will use "1" as the default for now.
        return 1

    # create a Contract instance and then pair it with an Order instance,
    # which will be sent to the IB API
    def create_contract(self, symbol, sec_type, exchange="SMART", currency="USD"):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = sec_type
        contract.exchange = exchange
        contract.currency = currency
        return contract

    # Overwrite EClient.historicalData
    def historicalData(self, reqId, bar):
        print(f"Time: {bar.date} Close: {bar.close}")
        self.hist_data.append([bar.date, bar.close])
        # if eurusd_contract.symbol in my_td_broker.hist_data.keys():
        #     my_td_broker.hist_data[eurusd_contract.symbol].append()

    def _error_handler(self, msg):
        # Handle open order orderId processing
        if (
            msg.typeName == "openOrder"
            and msg.orderId == self.order_id
            and not self.fill_dict.has_key(msg.orderId)
        ):
            self.create_fill_dict_entry(msg)
        # Handle Fills
        elif (
            msg.typeName == "orderStatus"
            and msg.status == "Filled"
            and self.fill_dict[msg.orderId]["filled"] == False
        ):
            self.create_fill(msg)
        print("Server Response: {}, {}\n".format(msg.typeName, msg))

    def register_handlers(self):
        """
        Register the error and server reply  message handling functions.
        """
        # Assign the error handling function defined above
        # to the TWS connection
        self.tws_conn.register(self._error_handler, "Error")

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
            "filled": False,
        }

    def create_fill(self, msg):
        fd = self.fill_dict[msg.orderId]
        symbol = fd["symbol"]
        exchange = fd["exchange"]
        filled = msg.filled
        direction = fd["direction"]
        fill_cost = msg.avgFillPrice

        fill = FillEvent(
            datetime.datetime.utcnow(), symbol, exchange, filled, direction, fill_cost
        )

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
                asset, asset_type, self.order_routing, self.order_routing, self.currency
            )

            # Create the Interactive Brokers order via the
            # passed Order event
            ib_order = self.create_order(order_type, quantity, direction)

            # Use the connection to the send the order to IB
            self.tws_conn.placeOrder(self.order_id, ib_contract, ib_order)

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


# not working. Seems like a developer account needs to be created with TDA
class TDABroker(Broker):
    def __init__(self, events) -> None:
        super(TDABroker, self).__init__()
        self.events = events
        self.consumer_key = os.environ["TDD_consumer_key"]
        self.account_id = os.environ["TDA_account_id"]
        self.access_token = None
        self.refresh_token = None
        self.get_token("authorization")

    def _signin_code(self):
        import selenium
        from selenium import webdriver
        from backtest.utilities.utils import load_credentials, parse_args

        args = parse_args()
        load_credentials(args.credentials)

        driver = webdriver.Firefox()
        url = f"https://auth.tdameritrade.com/auth?response_type=code&redirect_uri=http://localhost&client_id={self.consumer_key}@AMER.OAUTHAP"
        driver.get(url)

        userId = driver.find_element_by_css_selector("#username0")
        userId.clear()
        userId.send_keys(os.environ["TDA_username"])
        pw = driver.find_element_by_css_selector("#password1")
        pw.clear()
        pw.send_keys(f"{os.environ['TDA_pw']}")
        login_button = driver.find_element_by_css_selector("#accept")
        login_button.click()

        # click accept
        accept_button = driver.find_element_by_css_selector("#accept")
        try:
            accept_button.click()
        except selenium.common.exceptions.WebDriverException:
            new_url = driver.current_url
            code = new_url.split("code=")[1]
            logging.info("Coded:\n"+code)
            return code
        finally:
            driver.close()

    def get_token(self, grant_type):
        import urllib

        if grant_type == "authorization":
            code = self._signin_code()
            if code is not None:
                code = urllib.parse.unquote(code)
                logging.info("Decoded:\n"+code)
                params = {
                    "grant_type": "authorization_code",
                    "access_type": "offline",
                    "code": code,
                    "client_id": self.consumer_key,
                    "redirect_uri": "http://localhost",
                }
                headers = {"Content-Type": "application/x-www-form-urlencoded"}
                res = requests.post(
                    r"https://api.tdameritrade.com/v1/oauth2/token",
                    headers=headers,
                    data=params,
                )
                if res.ok:
                    res_body = res.json()
                    logging.info("Obtained access_token & refresh_token")
                    self.access_token = res_body["access_token"]
                    self.refresh_token = res_body["refresh_token"]
                else:
                    print(res)
                    print(res.json())
                    raise Exception(
                        f"API POST exception: Error {res.status_code}")
            else:
                raise Exception("Could not sign in and obtain code")
        elif grant_type == "refresh":
            params = {
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": self.consumer_key,
            }
            res = requests.post(
                r"https://api.tdameritrade.com/v1/oauth2/token",
                data=params,
            )
            if res.ok:
                res_body = res.json()
                self.access_token = res_body["access_token"]
                print(res_body["access_token"])
            else:
                print(res.json())

    def get_account_details(self):
        return requests.get(
            f"https://api.tdameritrade.com/v1/accounts/{os.environ['TDA_account_id']}",
            headers={"Authorization": f"Bearer {self.access_token}"},
        ).json()

    def get_quote(self, symbol):
        return requests.get(
            f"https://api.tdameritrade.com/v1/marketdata/{symbol}/quotes",
            params={"apikey": self.consumer_key},
        ).json()

    def calculate_commission(self):
        return 0

    def execute_order(self, event: OrderEvent) -> bool:
        data = {
            "orderType": "MARKET" if event.order_type == OrderType.MARKET else "LIMIT",
            "session": "NORMAL",
            "duration": "DAY",
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [{
                "instruction": "BUY" if event.direction == OrderPosition.BUY else "SELL",
                "quantity": event.quantity,
                "instrument": {
                    "symbol": event.symbol,
                    "assetType": "EQUITY"
                }
            }]
        }
        if data["orderType"] == "LIMIT":
            data["price"] = event.signal_price
        res = requests.post(
            f"https://api.tdameritrade.com/v1/accounts/{self.account_id}/orders",
            data=json.dumps(data),
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            }
        )
        if res.ok:
            fill_event = FillEvent(event, self.calculate_commission())
            self.events.put(fill_event)
            return True
        print(
            f"Place Order Unsuccessful: {event.order_details()}\n{res.status_code}\n{res.json()}")
        print(res.text)
        return False

    def cancel_order(self, order_id) -> bool:
        # NOTE: Unused and only skeleton.
        # TODO: Implement while improving TDABroker class
        res = requests.delete(
            f"https://api.tdameritrade.com/v1/accounts/{self.account_id}/orders/{order_id}",
            headers={"Authorization": f"Bearer {self.access_token}"}
        )
        if res.ok:
            return True
        return False

    def _filter_execute_order(self, event: OrderEvent) -> bool:
        return True

    def get_past_transactions(self):
        return requests.get(
            f"https://api.tdameritrade.com/v1/accounts/{self.account_id}/transactions",
            params={
                "type": "ALL",
            },
            headers={"Authorization": f"Bearer {self.access_token}"}
        ).json()


class AlpacaBroker(Broker):
    def __init__(self, event_queue):
        self.events = event_queue
        self.base_url = "https://paper-api.alpaca.markets"
        self.data_url = "https://data.alpaca.markets/v2"
        self.api = alpaca_trade_api.REST(
            os.environ["alpaca_key_id"],
            os.environ["alpaca_secret_key"],
            self.base_url,
            api_version="v2",
        )

    def _alpaca_endpoint(self, url, args: str):
        return requests.get(
            url + args,
            headers={
                "APCA-API-KEY-ID": os.environ["alpaca_key_id"],
                "APCA-API-SECRET-KEY": os.environ["alpaca_secret_key"],
            }
        ).json()

    """ ORDERS """

    def get_current_orders(self):
        return self.api.list_orders()

    def _filter_execute_order(self, event: OrderEvent) -> bool:
        return True

    def execute_order(self, event: OrderEvent) -> bool:
        side = "buy" if event.direction == OrderPosition.BUY else "sell"
        try:
            if event.order_type == OrderType.LIMIT:
                order = self.api.submit_order(
                    symbol=event.symbol,
                    qty=event.quantity,
                    side=side,
                    type="limit",
                    time_in_force="day",
                    limit_price=event.signal_price,
                )
                event.trade_price = event.signal_price
            else:
                # todo: figure out a way to get trade_price for market orders
                order = self.api.submit_order(
                    symbol=event.symbol,
                    qty=event.quantity,
                    side=side,
                    type="market",
                    time_in_force="day",
                )
                event.trade_price = event.signal_price
        except alpaca_trade_api.rest.APIError as e:
            log_message(f"{self.api.get_account()}")
            log_message(
                f"Status Code [{e.status_code}] {e.code}: {str(e)}\nResponse: {e.response}")
            return False
        if order.status == "accepted":
            fill_event = FillEvent(event, self.calculate_commission())
            self.events.put(fill_event)
            return True
        return False

    def calculate_commission(self):
        return 0

    """ PORTFOLIO RELATED """

    def get_positions(self):
        return self.api.list_positions()

    def get_historical_bars(
        self, ticker, timeframe, start, end, limit: int = None
    ) -> pd.DataFrame:
        assert timeframe in ["1Min", "5Min", "15Min", "day", "1D"]
        if limit is not None:
            return self.api.get_barset(
                ticker, timeframe, start=start, end=end, limit=limit
            ).df
        return self.api.get_barset(ticker, timeframe, start=start, end=end).df

    def get_quote(self, ticker):
        return self.api.get_last_quote(ticker)
