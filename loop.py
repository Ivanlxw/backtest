import queue
import random
import logging
import talib

from backtest.broker import SimulatedBroker
from backtest.data.dataHandler import HistoricCSVDataHandler
from backtest.strategy.fundamental import FundamentalFScoreStrategy
from backtest.strategy.statistics import BuyDips, DipswithTA
from backtest.portfolio.portfolio import PercentagePortFolio
from backtest.portfolio.strategy import LongOnly
from backtest.utilities.backtest import backtest
from backtest.utilities.utils import load_credentials, parse_args, remove_bs

args = parse_args()

if args.name != "":
    logging.basicConfig(filename=args.name+'.log', level=logging.INFO)

with open("data/downloaded_universe.txt", 'r') as fin:
    stock_list = fin.readlines()
stock_list = list(map(remove_bs, stock_list))
symbol_list = random.sample(stock_list, 25)
symbol_list += ["DUK", "JPM", "TXN"]
symbol_list = set(symbol_list)  ## move duplicate

load_credentials(args.credentials)

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
start_date = "2015-01-02"  ## YYYY-MM-DD

bars = HistoricCSVDataHandler(event_queue, csv_dir="data/data/daily",
                                           symbol_list=symbol_list, 
                                           start_date=start_date,
                                           fundamental=args.fundamental
                                           )
# strategy = DoubleMAStrategy(bars, event_queue, [14,50], talib.SMA)
# strategy = CustomRSI(bars, event_queue, 20, 50)
strategy = BuyDips(bars, event_queue, 7, 40)

if args.fundamental:
    strategy = FundamentalFScoreStrategy(bars, event_queue)
port = PercentagePortFolio(bars, event_queue, order_queue, 
    percentage=0.10, portfolio_name="BuyDips",
    mode='asset',
    expires=7
)
broker = SimulatedBroker(bars, port, event_queue, order_queue)

backtest(symbol_list, bars, event_queue, order_queue, strategy, port, broker, start_date=start_date)
