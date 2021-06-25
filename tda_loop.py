"""
Actual file to run for backtesting 
"""
import queue
import random
import talib
import logging

from backtest.broker import SimulatedBroker, TDABroker
from trading.data.dataHandler import TDAData
from backtest.portfolio.portfolio import PercentagePortFolio
from backtest.portfolio.strategy import LongOnly
from backtest.strategy.statistics import BuyDips
from backtest.strategy.ta import MeanReversionTA
from trading.utilities.utils import load_credentials, parse_args, remove_bs
from backtest.utilities.backtest import backtest

args = parse_args()
load_credentials(args.credentials)
if args.name != "":
    logging.basicConfig(filename=args.name + ".log", level=logging.INFO)

with open("data/downloaded_universe.txt", "r") as fin:
    stock_list = fin.readlines()
stock_list_downloaded = list(map(remove_bs, stock_list))

with open("data/dow_stock_list.txt", "r") as fin:
    stock_list = fin.readlines()
dow_stock_list = list(map(remove_bs, stock_list))

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
symbol_list = random.sample(stock_list_downloaded, 15) + random.sample(
    dow_stock_list, 10
)
symbol_list = set(symbol_list)
# Declare the components with respective parameters
# broker = AlpacaBroker()
NY = "America/New_York"
SG = "Singapore"
live = False
start_date = "2017-04-05" if not live else None

bars = TDAData(event_queue, ["TSLA", "AMZN"],
               start_date, period_type="month", period=6, frequency_type="daily", frequency=1)

strategy = MeanReversionTA(
    bars, event_queue, timeperiod=14, ma_type=talib.SMA, sd=2, exit=True
)

port = PercentagePortFolio(
    bars,
    event_queue,
    order_queue,
    percentage=0.10,
    mode="asset",
    expires=7,
    portfolio_name="tda_loop",
    portfolio_strategy=LongOnly
)

if live:
    broker = TDABroker(event_queue)
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
