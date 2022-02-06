import logging
import os
import queue
from pathlib import Path

from Data.DataWriters.Prices import ABSOLUTE_BT_DATA_DIR
from backtest.strategy import profitable
from backtest.utilities.backtest import backtest
from backtest.utilities.utils import load_credentials, parse_args, remove_bs
from trading.broker.broker import TDABroker
from trading.broker.gatekeepers import EnoughCash, MaxPortfolioPosition, NoShort, PremiumLimit
from trading.data.dataHandler import DataFromDisk
from trading.portfolio.portfolio import NaivePortfolio
from trading.portfolio.rebalance import RebalanceLogicalAny, RebalanceYearly, SellLosersHalfYearly
from trading.strategy.basic import OneSidedOrderOnly
from trading.utilities.enum import OrderPosition, OrderType
from trading.utilities.utils import ETF_LIST

args = parse_args()
load_credentials(args.credentials)
if args.name != "":
    logging.basicConfig(filename=Path(os.environ['WORKSPACE_ROOT']) /
                        f"Data/logging/{args.name}.log", level=logging.INFO, force=True)
event_queue = queue.LifoQueue()
order_queue = queue.Queue()
bars = DataFromDisk(event_queue, ETF_LIST[:3], "2021-01-05", live=True)
strategy = profitable.comprehensive_longshort(bars, event_queue)
rebalance_strat = RebalanceLogicalAny(bars, event_queue, [
    SellLosersHalfYearly(bars, event_queue),
    RebalanceYearly(bars, event_queue)
])
port = NaivePortfolio(
    bars,
    event_queue,
    order_queue,
    2,
    portfolio_name=(args.name if args.name != "" else "tda_loop"),
    order_type=OrderType.LIMIT,
    rebalance=rebalance_strat
)
broker = TDABroker(event_queue, gatekeepers=[EnoughCash(bars), NoShort(
    bars), MaxPortfolioPosition(bars, 10), PremiumLimit(bars, 150.0)])

def test_tda_stuff():
    strategy = OneSidedOrderOnly(bars, event_queue, OrderPosition.BUY)
    return


if __name__ == "__main__":
    backtest(
        bars, event_queue, order_queue,
        strategy, port, broker, loop_live=True, sleep_duration=args.sleep_time)
