"""
Actual file to run for backtesting 
"""

from backtest.utilities.backtest import backtest
from backtest.data.dataHandler import AlpacaData
import queue
import random
import talib
import logging

from backtest.broker import AlpacaBroker
from backtest.portfolio.base import PercentagePortFolio
from backtest.portfolio.strategy import DefaultOrder
from backtest.strategy.ta import MeanReversionTA
from backtest.utilities.utils import load_credentials, parse_args, remove_bs

args = parse_args()
if args.name != "":
    logging.basicConfig(filename=args.name+'.log', level=logging.INFO)

with open("data/dow_stock_list.txt", 'r') as fin:
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

bars = AlpacaData(event_queue, symbol_list, live=live)
strategy = MeanReversionTA(
    bars, event_queue, timeperiod=14, 
    ma_type=talib.SMA, sd=2, exit=True
)

port = PercentagePortFolio(bars, event_queue, order_queue, 
    percentage=0.05, 
    mode='asset',
    portfolio_strategy=DefaultOrder,
    expires=7,
    portfolio_name="alpaca_loop"
)

broker = AlpacaBroker()

if live:
    backtest(symbol_list, 
            bars, event_queue, order_queue, 
            strategy, port, broker, loop_live=True)
