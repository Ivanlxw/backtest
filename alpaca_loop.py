"""
Actual file to run for backtesting
"""
import os
import queue
import logging
from trading.strategy.statistics import ExtremaBounce, LongTermCorrTrend
import talib

from backtest.broker import AlpacaBroker, SimulatedBroker
from backtest.portfolio.portfolio import PercentagePortFolio
from backtest.portfolio.rebalance import SellLongLosers
from backtest.portfolio.strategy import DefaultOrder, LongOnly
from backtest.utilities.backtest import backtest
from trading.data.dataHandler import AlpacaData
from trading.utilities.utils import load_credentials, parse_args, remove_bs
from trading.strategy.ta import BoundedTA, ExtremaTA, MeanReversionTA, TAIndicatorType
from trading.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy
from trading.utilities.enum import OrderType

args = parse_args()
load_credentials(args.credentials)
if args.name != "":
    logging.basicConfig(filename=args.name + ".log", level=logging.INFO)

with open(f"{os.path.abspath(os.path.dirname(__file__))}/data/dow_stock_list.txt", 'r') as fin:
    stock_list = fin.readlines()
stock_list = list(map(remove_bs, stock_list))


with open(f"{os.path.abspath(os.path.dirname(__file__))}/data/snp500.txt", 'r') as fin:
    snp500 = fin.readlines()
stock_list += list(map(remove_bs, snp500))


symbol_list = list(set(stock_list))
event_queue = queue.LifoQueue()
order_queue = queue.Queue()
# Declare the components with respective parameters
NY = "America/New_York"
SG = "Singapore"
start_date = "2017-04-05" if not live else None

bars = AlpacaData(event_queue, symbol_list, live=live, start_date=start_date)

# strategy = MultipleAllStrategy([
#     BoundedTA(bars, event_queue,
#               period=7, ta_period=20,
#               floor=-100.0, ceiling=140.0,
#               ta_indicator=talib.CCI, ta_indicator_type=TAIndicatorType.ThreeArgs
#               ),
#     BoundedTA(bars, event_queue,
#               period=7, ta_period=14,
#               floor=30.0, ceiling=70.0,
#               ta_indicator=talib.RSI, ta_indicator_type=TAIndicatorType.TwoArgs
#               ),
#     ExtremaTA(bars, event_queue,
#               ta_indicator=talib.RSI, ta_period=14, ta_indicator_type=TAIndicatorType.TwoArgs,
#               extrema_period=10, consecutive=2
#               )
# ])

strategy = MultipleAllStrategy([
    ExtremaBounce(bars, 7, 60),
    ExtremaTA(bars, event_queue, talib.RSI, 14,
              TAIndicatorType.TwoArgs, extrema_period=10, consecutive=2),
    # BoundedTA(bars, events, 7, 14, floor=32, ceiling=65,
    #           ta_indicator=talib.RSI, ta_indicator_type=TAIndicatorType.TwoArgs)
    LongTermCorrTrend(bars, event_queue, 80)
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
    portfolio_strategy=LongOnly,
    rebalance=SellLongLosers
)

if args.live:
    broker = AlpacaBroker(event_queue)
    backtest(
        symbol_list,
        bars,
        event_queue,
        order_queue,
        strategy,
        port,
        broker,
        loop_live=args.live,
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
        loop_live=args.live,
        start_date=start_date,
    )
