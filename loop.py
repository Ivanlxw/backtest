import queue
import random
import logging
import talib

from backtest.broker import SimulatedBroker
from backtest.data.dataHandler import HistoricCSVDataHandler
from backtest.strategy.fundamental import FundamentalFScoreStrategy
from backtest.strategy.ta import CustomRSI, DoubleMAStrategy
from backtest.portfolio.base import PercentagePortFolio
from backtest.portfolio.strategy import DefaultOrder
from backtest.utilities.backtest import backtest, plot_benchmark
from backtest.utilities.utils import load_credentials, parse_args, remove_bs

args = parse_args()

if args.name != "":
    logging.basicConfig(filename=args.name+'.log', level=logging.INFO)

with open("data/downloaded_universe.txt", 'r') as fin:
    stock_list = fin.readlines()
stock_list = list(map(remove_bs, stock_list))
symbol_list = random.sample(stock_list, 12)

load_credentials(args.credentials)
event_queue = queue.LifoQueue()
order_queue = queue.Queue()
start_date = "2010-01-04"  ## YYYY-MM-DD

bars = HistoricCSVDataHandler(event_queue, csv_dir="data/data/daily",
                                           symbol_list=symbol_list, 
                                           start_date=start_date,
                                           fundamental=args.fundamental
                                           )
strategy = DoubleMAStrategy(bars, event_queue, [14,50], talib.SMA)
# strategy = CustomRSI(bars, event_queue, 20, 50)

if args.fundamental:
    strategy = FundamentalFScoreStrategy(bars, event_queue)
port = PercentagePortFolio(bars, event_queue, order_queue, 
    percentage=0.05, portfolio_name="DoubleMA",
    mode='asset',
    portfolio_strategy=DefaultOrder,
    expires=7
)
broker = SimulatedBroker(bars, event_queue, order_queue)

backtest(symbol_list, bars, event_queue, order_queue, strategy, port, broker, start_date=start_date)