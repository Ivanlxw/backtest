import queue
import os
import random
import logging
import os
import talib
import pandas as pd

from backtest.utilities.utils import generate_start_date, parse_args, remove_bs, load_credentials
from backtest.broker import SimulatedBroker
from backtest.portfolio.rebalance import BaseRebalance, SellLongLosers
from backtest.portfolio.portfolio import PercentagePortFolio
from backtest.portfolio.strategy import DefaultOrder, LongOnly, ProgressiveOrder
from backtest.utilities.backtest import backtest
from backtest.strategy.fundamental import FundamentalFScoreStrategy
from trading.data.dataHandler import HistoricCSVDataHandler
from trading.strategy.multiple import MultipleAllStrategy
from trading.strategy.ta import BoundedTA, ExtremaTA, TAIndicatorType
from trading.strategy.statistics import ExtremaBounce, LongTermCorrTrend, RelativeExtrema

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
start_date = generate_start_date() 
while pd.Timestamp(start_date).dayofweek > 4:
    start_date = generate_start_date() 
print(start_date)
end_date = "2020-01-30"
csv_dir = os.path.abspath(
    os.path.dirname(__file__))+"/data/data/daily"
symbol_list = list(set(
    random.sample([
        fn.replace('.csv', '') for fn in os.listdir(csv_dir)
    ], 75) + ["DUK", "AON", "C", "UAL", "AMZN", "COG"]))
bars = HistoricCSVDataHandler(event_queue,
                              csv_dir,
                              symbol_list,
                              start_date=start_date,
                              end_date=end_date
                              )

strategy = MultipleAllStrategy([
    RelativeExtrema(bars, event_queue, 
        long_time=100, 
        percentile=10, strat_contrarian=True),
    LongTermCorrTrend(bars, event_queue, 100, corr=0.4, strat_contrarian=False),
    BoundedTA(bars, event_queue, 7, 20, floor=30, ceiling=70,
              ta_indicator=talib.RSI, ta_indicator_type=TAIndicatorType.TwoArgs),  
])

if args.fundamental:
    strategy = FundamentalFScoreStrategy(bars, event_queue)
port = PercentagePortFolio(bars, event_queue, order_queue,
                           percentage=0.10,
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
