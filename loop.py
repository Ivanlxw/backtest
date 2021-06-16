import queue
import random
import logging
import talib

from backtest.broker import SimulatedBroker
from backtest.data.dataHandler import HistoricCSVDataHandler
from backtest.portfolio.portfolio import PercentagePortFolio
from backtest.portfolio.strategy import LongOnly
from backtest.strategy.fundamental import FundamentalFScoreStrategy
from backtest.strategy.statistics import BuyDips
from backtest.strategy.ta import BoundedTA, SimpleTACross, TAIndicatorType
from backtest.utilities.backtest import backtest
from backtest.utilities.utils import load_credentials, parse_args, remove_bs
from backtest.strategy.multiple import MultipleAllStrategy
from backtest.portfolio.rebalance import BaseRebalance

args = parse_args()

if args.name != "":
    logging.basicConfig(filename=args.name+'.log', level=logging.INFO)

with open("data/dow_stock_list.txt", 'r') as fin:
    stock_list = fin.readlines()
stock_list = list(map(remove_bs, stock_list))
symbol_list = random.sample(stock_list, 10)

load_credentials(args.credentials)

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
start_date = "2018-01-02"  # YYYY-MM-DD

bars = HistoricCSVDataHandler(event_queue, csv_dir="data/data/daily",
                              symbol_list=symbol_list,
                              start_date=start_date,
                              fundamental=args.fundamental
                              )
# strategy = MultipleAnyStrategy([
#     BuyDips(
#         bars, event_queue, short_time=80, long_time=150
#     ),
#     SimpleTACross(bars, event_queue, timeperiod=20, ma_type=talib.SMA)
# ])
strategy = MultipleAllStrategy([
    BoundedTA(bars, event_queue, 10, 20, -95, 150,
              talib.CCI, ta_indicator_type=TAIndicatorType.ThreeArgs),
    BoundedTA(bars, event_queue, 7, 14, 35.0, 70.0,
              talib.RSI, ta_indicator_type=TAIndicatorType.TwoArgs)
])

if args.fundamental:
    strategy = FundamentalFScoreStrategy(bars, event_queue)
port = PercentagePortFolio(bars, event_queue, order_queue,
                           percentage=0.15,
                           portfolio_name=(
                               args.name if args.name != "" else "loop"),
                           mode='asset',
                           expires=7,
                           rebalance=BaseRebalance,
                           portfolio_strategy=LongOnly
                           )
broker = SimulatedBroker(bars, port, event_queue, order_queue)

backtest(symbol_list, bars, event_queue, order_queue,
         strategy, port, broker, start_date=start_date)
