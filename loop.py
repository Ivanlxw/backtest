import queue
import os
import random
import logging
import os
import talib

from backtest.broker import SimulatedBroker
from backtest.portfolio.rebalance import BaseRebalance, SellLongLosers
from backtest.portfolio.portfolio import PercentagePortFolio
from backtest.portfolio.strategy import LongOnly
from backtest.utilities.backtest import backtest
from backtest.strategy.fundamental import FundamentalFScoreStrategy
from trading''.data.dataHandler import HistoricCSVDataHandler
from trading''.strategy.multiple import MultipleAllStrategy
from trading''.strategy.ta import BoundedTA, ExtremaTA, TAIndicatorType
from trading''.strategy.statistics import ExtremaBounce, LongTermCorrTrend
from trading''.utilities.utils import parse_args, remove_bs, load_credentials

args = parse_args()

if args.name != "":
    logging.basicConfig(filename=args.name+'.log', level=logging.INFO)

with open(f"{os.path.abspath(os.path.dirname(__file__))}/data/dow_stock_list.txt", 'r') as fin:
    stock_list = fin.readlines()
stock_list = list(map(remove_bs, stock_list))
symbol_list = stock_list

load_credentials(args.credentials)

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
# YYYY-MM-DD
start_date = f"{random.randint(2005, 2019+1)}-{random.randint(1,13)}-05"

csv_dir = os.path.abspath(
    os.path.dirname(__file__))+"/data/data/daily"
symbol_list = list(set(
    random.sample([
        fn.replace('.csv', '') for fn in os.listdir(csv_dir)
    ], 75) + ["DUK", "JPM", "TXN", "UAL", "AMZN", "TSLA"]))
bars = HistoricCSVDataHandler(event_queue,
                              csv_dir,
                              symbol_list,
                              start_date=start_date,
                              )

strategy = MultipleAllStrategy([
    ExtremaBounce(bars, 7, 60),
    ExtremaTA(bars, event_queue, talib.RSI, 14,
              TAIndicatorType.TwoArgs,
              extrema_period=10, strat_contrarian=True,
              consecutive=2),
    BoundedTA(bars, event_queue, 7, 14, floor=32, ceiling=70,
              ta_indicator=talib.RSI, ta_indicator_type=TAIndicatorType.TwoArgs),
    LongTermCorrTrend(bars, event_queue, 120)
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
