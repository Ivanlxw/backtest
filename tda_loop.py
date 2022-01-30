import logging
import os
import queue
from pathlib import Path
from turtle import position
from backtest.utilities.backtest import backtest

from backtest.utilities.utils import load_credentials, parse_args
from trading.broker.broker import TDABroker
from trading.broker.gatekeepers import EnoughCash, NoShort
from trading.data.dataHandler import DataFromDisk
from trading.portfolio.portfolio import NaivePortfolio
from trading.portfolio.rebalance import RebalanceLogicalAny, RebalanceYearly, SellLosersHalfYearly
from trading.strategy.basic import OneSidedOrderOnly
from trading.utilities.enum import OrderPosition, OrderType

args = parse_args()
load_credentials(args.credentials)
if args.name != "":
    logging.basicConfig(filename=Path(os.environ['WORKSPACE_ROOT']) /
                        f"Data/logging/{args.name}.log", level=logging.INFO, force=True)

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
bars = DataFromDisk(event_queue, ["TSLA", "JPM"], "2022-01-05", live=True)
strategy = OneSidedOrderOnly(bars, event_queue, OrderPosition.BUY)
rebalance_strat = RebalanceLogicalAny(bars, event_queue, [
    SellLosersHalfYearly(bars, event_queue),
    RebalanceYearly(bars, event_queue)
])
port = NaivePortfolio(
    bars,
    event_queue,
    order_queue,
    1,
    portfolio_name=(args.name if args.name != "" else "tda_loop"),
    order_type=OrderType.LIMIT,
    rebalance=rebalance_strat
)
broker = TDABroker(event_queue, gatekeepers=[EnoughCash(bars), NoShort(bars)])

if __name__ == "__main__":
    backtest(
        bars, event_queue, order_queue,
        strategy, port, broker, loop_live=True, sleep_duration=args.sleep_time)
