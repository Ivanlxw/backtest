"""
Actual file to run for backtesting 
"""
from backtest.utilities.backtest import backtest
from backtest.data.dataHandler import AlpacaData
import queue
import random
import talib
import logging

from backtest.broker import AlpacaBroker, SimulatedBroker
from backtest.portfolio.portfolio import PercentagePortFolio
from backtest.portfolio.strategy import DefaultOrder, LongOnly
from backtest.strategy.statistics import BuyDips
from backtest.strategy.ta import MeanReversionTA
from backtest.utilities.utils import load_credentials, parse_args, remove_bs

args = parse_args()
if args.name != "":
    logging.basicConfig(filename=args.name+'.log', level=logging.INFO)

with open("data/downloaded_universe.txt", 'r') as fin:
    stock_list = fin.readlines()
stock_list = list(map(remove_bs, stock_list))

load_credentials(args.credentials)
            
event_queue = queue.LifoQueue()
order_queue = queue.Queue()
symbol_list = random.sample(stock_list, 25)

# Declare the components with respective parameters
# broker = AlpacaBroker()
NY = 'America/New_York'
SG = 'Singapore'
live = True
start_date = "2017-04-05" if not live else None

bars = AlpacaData(event_queue, symbol_list, live=live, start_date=start_date)
strategy = MeanReversionTA(
    bars, event_queue, timeperiod=14, 
    ma_type=talib.SMA, sd=2, exit=True
)

port = PercentagePortFolio(bars, event_queue, order_queue, 
    percentage=0.05, 
    mode='asset',
    expires=7,
    portfolio_name="alpaca_loop",
    portfolio_strategy=LongOnly
)
if live:
    broker = AlpacaBroker()
else:
    broker = SimulatedBroker(bars, port, event_queue, order_queue)
if live:
    backtest(symbol_list, 
            bars, event_queue, order_queue, 
            strategy, port, broker, loop_live=live)
else:
    backtest(symbol_list, 
            bars, event_queue, order_queue, 
            strategy, port, broker, loop_live=live, start_date=start_date)
