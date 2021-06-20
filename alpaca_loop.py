"""
Actual file to run for backtesting 
"""
import queue
import random
import logging
import talib

from backtest.broker import AlpacaBroker, SimulatedBroker
from backtest.portfolio.portfolio import PercentagePortFolio
from backtest.portfolio.rebalance import SellLongLosers
from backtest.strategy.statistics import BuyDips
from backtest.strategy.ta import BoundedTA, ExtremaTA, MeanReversionTA, TAIndicatorType
from backtest.portfolio.strategy import DefaultOrder, ProgressiveOrder
from backtest.utilities.backtest import backtest
from backtest.data.dataHandler import AlpacaData
from trading_common.utilities.utils import load_credentials, parse_args, remove_bs
from trading_common.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy
from trading_common.utilities.enum import OrderType

args = parse_args()
load_credentials(args.credentials)
if args.name != "":
    logging.basicConfig(filename=args.name + ".log", level=logging.INFO)

with open("data/dow_stock_list.txt", "r") as fin:
    stock_list = fin.readlines()
dow_stock_list = list(map(remove_bs, stock_list))

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
symbol_list = random.sample(
    dow_stock_list, 20
)
# Declare the components with respective parameters
NY = "America/New_York"
SG = "Singapore"
live = True
start_date = "2017-04-05" if not live else None

bars = AlpacaData(event_queue, symbol_list, live=live, start_date=start_date)
# strategy = MultipleAnyStrategy([
#     BuyDips(
#         bars, event_queue, short_time=80, long_time=150
#     ),
#     BoundedTA(bars, event_queue,
#               period=10, ta_period=14,
#               floor=37.0, ceiling=70.0,
#               ta_indicator=talib.RSI, ta_indicator_type=TAIndicatorType.TwoArgs
#               )
# ])

strategy = MultipleAllStrategy([
    BoundedTA(bars, event_queue,
              period=7, ta_period=20,
              floor=-95.0, ceiling=140.0,
              ta_indicator=talib.CCI, ta_indicator_type=TAIndicatorType.ThreeArgs
              ),
    BoundedTA(bars, event_queue,
              period=7, ta_period=14,
              floor=37.0, ceiling=70.0,
              ta_indicator=talib.RSI, ta_indicator_type=TAIndicatorType.TwoArgs
              ),
    ExtremaTA(bars, event_queue,
              ta_indicator=talib.RSI, ta_period=14, ta_indicator_type=TAIndicatorType.TwoArgs,
              extrema_period=10, consecutive=2
              )
])

port = PercentagePortFolio(
    bars,
    event_queue,
    order_queue,
    percentage=0.05,
    mode="asset",
    expires=3,
    portfolio_name=(args.name if args.name != "" else "alpaca_loop"),
    order_type=OrderType.MARKET,
    portfolio_strategy=DefaultOrder,
    rebalance=SellLongLosers
)

if live:
    broker = AlpacaBroker(event_queue)
    backtest(
        symbol_list,
        bars,
        event_queue,
        order_queue,
        strategy,
        port,
        broker,
        loop_live=live,
    )
else:
    broker = SimulatedBroker(bars, port, event_queue, order_queue)
    backtest(
        symbol_list,
        bars,
        event_queue,
        order_queue,
        strategy,
        port,
        broker,
        loop_live=live,
        start_date=start_date,
    )
