"""Manual IBKR smoke test: place a limit order priced far from the market so it is accepted but never fills.

Defaults to BUY 1 COIN @ $1.00 on the paper-trading port. Live ports are refused.
Needs TWS / IB Gateway running with the API enabled. NOT run by CI or the verification checklist.

    uv run python test/send_unfillable_order.py                  # TWS paper (7497)
    uv run python test/send_unfillable_order.py --port 4002      # IB Gateway paper

The order is left working (DAY) when the script exits. Cancel it with backtest/utilities/ibkr/cancel_orders.py.
"""
import argparse
import threading

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.wrapper import EWrapper

PAPER_PORTS = {7497: "TWS paper", 4002: "IB Gateway paper"}
LIVE_PORTS = {7496: "TWS live", 4001: "IB Gateway live"}


class UnfillableOrderApp(EClient, EWrapper):
    def __init__(self, symbol: str, action: str, qty: int, price: float, wait_s: float):
        EClient.__init__(self, self)
        self.symbol, self.action, self.qty, self.price, self.wait_s = symbol, action, qty, price, wait_s

    def nextValidId(self, orderId: int):
        contract = Contract()
        contract.symbol = self.symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        order = Order()
        order.action = self.action
        order.orderType = "LMT"
        order.lmtPrice = self.price
        order.totalQuantity = self.qty
        order.tif = "DAY"
        # ibapi 10.x defaults these to True-ish values some TWS versions reject (error 10268)
        order.eTradeOnly = False
        order.firmQuoteOnly = False

        print(f"placing {self.action} {self.qty} {self.symbol} LMT {self.price} (orderId={orderId})")
        self.placeOrder(orderId, contract, order)
        threading.Timer(self.wait_s, self.disconnect).start()

    def openOrder(self, orderId, contract, order, orderState):
        print(f"openOrder: id={orderId} {contract.symbol} {order.action} {order.totalQuantity} "
              f"@ {order.lmtPrice} state={orderState.status}")

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId,
                    lastFillPrice, clientId, whyHeld, mktCapPrice):
        print(f"orderStatus: id={orderId} status={status} filled={filled} remaining={remaining}")

    def error(self, reqId, errorTime, errorCode, errorString, advancedOrderRejectJson=""):
        # 2104/2106/2158 are "data farm connection OK" notices, not errors
        print(f"error: reqId={reqId} code={errorCode} {errorString}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=7497)
    ap.add_argument("--client-id", type=int, default=101)
    ap.add_argument("--symbol", default="COIN")
    ap.add_argument("--action", default="BUY", choices=["BUY", "SELL"])
    ap.add_argument("--qty", type=int, default=1)
    ap.add_argument("--price", type=float, default=1.0)
    ap.add_argument("--wait", type=float, default=10.0, help="seconds to listen for status before disconnecting")
    args = ap.parse_args()

    if args.port in LIVE_PORTS:
        ap.error(f"port {args.port} is {LIVE_PORTS[args.port]}; this script only targets paper accounts")
    if args.port not in PAPER_PORTS:
        ap.error(f"port {args.port} is not a known paper port {sorted(PAPER_PORTS)}")

    app = UnfillableOrderApp(args.symbol, args.action, args.qty, args.price, args.wait)
    app.connect(args.host, args.port, args.client_id)
    app.run()


if __name__ == "__main__":
    main()
