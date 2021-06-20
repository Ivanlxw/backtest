"""
Actual file to run for backtesting 
"""
import time
import queue
import random
import talib
import logging

from backtest.broker import SimulatedBroker
from trading_common.utilities.utils import load_credentials, parse_args, remove_bs
from backtest.data.dataHandler import HistoricCSVDataHandler
from backtest.portfolio.portfolio import PercentagePortFolio
from backtest.strategy.stat_data import BaseStatisticalData
from backtest.strategy.statistics import RawRegression
from backtest.utilities.backtest import backtest

## sklearn modules
from sklearn.linear_model import LinearRegression

args = parse_args()
if args.name != "":
    logging.basicConfig(filename=args.name+'.log', level=logging.INFO)

with open("data/downloaded_universe.txt", 'r') as fin:
    stock_list = fin.readlines()

load_credentials(args.credentials)

stock_list = list(map(remove_bs, stock_list))

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
start_date = "2015-01-01"  ## YYYY-MM-DD
symbol_list = random.sample(stock_list, 15)

start = time.time()
# Declare the components with respective parameters
## bars_test dates should not overlap with bars_train
bars = HistoricCSVDataHandler(event_queue, csv_dir="data/data/daily",
                                           symbol_list=symbol_list,
                                           start_date=start_date,
                                           )

strategy = RawRegression(
    bars, event_queue, LinearRegression, 
    BaseStatisticalData(bars, 30, 2, add_ta={
        'RSI': [talib.RSI, 14]
    }), 100
)
port = PercentagePortFolio(bars, event_queue, order_queue, 
                        percentage=0.03, portfolio_name="sk_reg")
broker = SimulatedBroker(bars, event_queue, order_queue)

backtest(
    symbol_list, bars, event_queue, order_queue, strategy, 
    port, broker, start_date=start_date)