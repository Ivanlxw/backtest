"""
Actual file to run for backtesting 
"""
import time
import queue
import random
import logging
from sklearn.ensemble import RandomForestClassifier

from backtest.broker import SimulatedBroker
from trading_common.utilities.utils import load_credentials, parse_args, remove_bs
from trading_common.data.dataHandler import HistoricCSVDataHandler
from backtest.portfolio.portfolio import PercentagePortFolio
from backtest.portfolio.rebalance import BaseRebalance
from backtest.strategy.stat_data import ClassificationData
from backtest.strategy.statistics import RawClassification
from backtest.utilities.backtest import backtest

args = parse_args()
if args.name != "":
    logging.basicConfig(filename=args.name+'.log', level=logging.INFO)

with open("data/stock_universe.txt", 'r') as fin:
    stock_list = fin.readlines()

load_credentials(args.credentials)
stock_list = list(map(remove_bs, stock_list))

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
start_date = "2000-01-25"  ## YYYY-MM-DD
symbol_list = random.sample(stock_list, 15)

start = time.time()
# Declare the components with respective parameters
## bars_test dates should not overlap with bars_train
bars = HistoricCSVDataHandler(event_queue, csv_dir="data/data/daily",
                                           symbol_list=symbol_list,
                                           start_date=start_date,
                                           end_date = "2010-12-31"
                                           )

strategy = RawClassification(bars, event_queue, RandomForestClassifier, processor=ClassificationData(bars, 14, 2), reoptimize_days=30)
port = PercentagePortFolio(bars, event_queue, order_queue, percentage=0.05, rebalance=BaseRebalance(event_queue))
broker = SimulatedBroker(bars, event_queue, order_queue)

backtest(symbol_list, bars, event_queue, order_queue, strategy, port, broker, start_date=start_date)
