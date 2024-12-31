from trading.broker.broker import IBBroker
from trading.portfolio.portfolio import FixedTradeValuePortfolio
from trading.portfolio.rebalance import NoRebalance
from trading.utilities.enum import OrderType


if __name__ == "__main__":
    print("just ctrl-a orders window and den delete button")
    # TODO: currently facing circular imports, FIX.
    gk = []
    portfolio = FixedTradeValuePortfolio(   # Prioxy
        trade_value=1,
        max_qty=1,
        portfolio_name="NIL",
        expires=1,
        rebalance=NoRebalance(),
        order_type=OrderType.LIMIT,
        initial_capital=100,
        load_portfolio_details=False,
    )
    broker = IBBroker(portfolio, gatekeepers=gk)
    broker.cancelAllOrders()
