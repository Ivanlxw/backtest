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
from backtest.utilities.utils import load_credentials, parse_args, remove_bs
from backtest.portfolio.strategy import DefaultOrder, ProgressiveOrder
from backtest.utilities.enums import OrderType
from backtest.utilities.backtest import backtest
from backtest.data.dataHandler import AlpacaData
from backtest.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy

args = parse_args()
load_credentials(args.credentials)
if args.name != "":
    logging.basicConfig(filename=args.name + ".log", level=logging.INFO)

with open("data/snp500.txt", "r") as fin:
    stock_list = fin.readlines()
stock_list_downloaded = list(map(remove_bs, stock_list))

with open("data/dow_stock_list.txt", "r") as fin:
    stock_list = fin.readlines()
dow_stock_list = list(map(remove_bs, stock_list))

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
symbol_list = random.sample(stock_list_downloaded, 120) + random.sample(
    dow_stock_list, 20
)
symbol_list = list(set(symbol_list))
# Declare the components with respective parameters
# broker = AlpacaBroker()
NY = "America/New_York"
SG = "Singapore"
live = True
start_date = "2017-04-05" if not live else None

bars = AlpacaData(event_queue, symbol_list, live=live, start_date=start_date)
# strategy = MeanReversionTA(
#     bars, event_queue, timeperiod=14, ma_type=talib.SMA, sd=2, exit=True
# )
strategy = MultipleAnyStrategy([
    BuyDips(
        bars, event_queue, short_time=80, long_time=150
    ),
    BoundedTA(bars, event_queue,
              period=10, ta_period=14,
              floor=37.0, ceiling=70.0,
              ta_indicator=talib.RSI, ta_indicator_type=TAIndicatorType.TwoArgs
              )
])

# strategy = MultipleAllStrategy([
#     BoundedTA(bars, event_queue,
#         period=7, ta_period=20,
#         floor=-95.0, ceiling=140.0,
#         ta_indicator=talib.CCI, ta_indicator_type=TAIndicatorType.ThreeArgs
#     ),
#     BoundedTA(bars, event_queue,
#         period=7, ta_period=14,
#         floor=37.0, ceiling=70.0,
#         ta_indicator=talib.RSI, ta_indicator_type=TAIndicatorType.TwoArgs
#     ),
#     ExtremaTA(bars, event_queue,
#         ta_indicator=talib.RSI, ta_period=14, ta_indicator_type=TAIndicatorType.TwoArgs,
#         extrema_period=10, consecutive=2
#     )
# ])

port = PercentagePortFolio(
    bars,
    event_queue,
    order_queue,
    percentage=0.05,
    mode="asset",
    expires=3,
    portfolio_name="alpaca_loop",
    order_type=OrderType.MARKET,
    portfolio_strategy=DefaultOrder,
    rebalance=SellLongLosers
)

# port = NaivePortfolio(
#     bars, event_queue, order_queue, 100, "alpaca_loop", initial_capital=150000,
#     expires = 3, portfolio_strategy=LongOnly
# )
if live:
    broker = AlpacaBroker()
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
