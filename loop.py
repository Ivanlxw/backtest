import queue
import os
import random
import logging
import os
import talib
import pandas as pd
from pathlib import Path

from backtest.utilities.utils import generate_start_date, parse_args, remove_bs, load_credentials
from backtest.broker import SimulatedBroker, TDABroker
from backtest.portfolio.rebalance import BaseRebalance, NoRebalance, SellLongLosersQuarterly, SellLongLosersYearly
from backtest.portfolio.portfolio import PercentagePortFolio
from backtest.portfolio.strategy import DefaultOrder, LongOnly, SellLowestPerforming
from backtest.utilities.backtest import backtest
from trading.strategy.fundamental import HighRevGain, LowDCF
from trading.data.dataHandler import FMPData, HistoricCSVDataHandler, TDAData
from trading.strategy.multiple import MultipleAllStrategy
from trading.strategy.ta import BoundedTA, ExtremaTA, MeanReversionTA, TAIndicatorType
from trading.strategy.statistics import ExtremaBounce, LongTermCorrTrend, RelativeExtrema
from trading.strategy.basic import BoundedPercChange, OneSidedOrderOnly
from trading.utilities.enum import OrderPosition

ABSOLUTE_FILEDIR = Path(os.path.dirname(os.path.abspath(__file__)))
args = parse_args()
load_credentials(args.credentials)

if args.name != "":
    logging.basicConfig(filename=ABSOLUTE_FILEDIR /
                        f"Data/logging/{args.name}.log", level=logging.INFO)
with open(f"{os.path.abspath(os.path.dirname(__file__))}/Data/downloaded_universe.txt", 'r') as fin:
    stock_list = fin.readlines()
stock_list = list(map(remove_bs, stock_list))
symbol_list = stock_list

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
# YYYY-MM-DD
start_date = generate_start_date()
while pd.Timestamp(start_date).dayofweek > 4:
    start_date = generate_start_date()
print(start_date)
end_date = "2020-01-30"


bars = HistoricCSVDataHandler(event_queue,
                              random.sample(symbol_list, 250),
                              start_date=start_date,
                              end_date=end_date
                              )


# strategy = MultipleAllStrategy([
#     RelativeExtrema(bars, event_queue,
#                     long_time=30,
#                     percentile=10, strat_contrarian=True),
#     BoundedTA(bars, event_queue, 7, 20, floor=30, ceiling=70,
#               ta_indicator=talib.RSI, ta_indicator_type=TAIndicatorType.TwoArgs),
#     BoundedTA(bars, event_queue, 7, 20, floor=-100, ceiling=100,
#               ta_indicator=talib.CCI, ta_indicator_type=TAIndicatorType.ThreeArgs),
#     OneSidedOrderOnly(bars, event_queue, OrderPosition.BUY)
# ])

strategy = MultipleAllStrategy([
    # OneSidedOrderOnly(bars, event_queue, OrderPosition.BUY),
    # LowDCF(bars, event_queue, buy_ratio=2, sell_ratio=6),
    HighRevGain(bars, event_queue, perc=3),
    RelativeExtrema(bars, event_queue,
                    long_time=100,
                    percentile=5, strat_contrarian=True),
    # BoundedTA(bars, event_queue, 7, 14, floor=30, ceiling=70,
    #           ta_indicator=talib.RSI, ta_indicator_type=TAIndicatorType.TwoArgs),
    # BoundedTA(bars, event_queue, 7, 20, floor=-100, ceiling=200,
    #           ta_indicator=talib.CCI, ta_indicator_type=TAIndicatorType.ThreeArgs),
])

# strategy = MultipleAllStrategy([
#     ExtremaBounce(bars, event_queue, 7, 100, percentile=10),
#     HighRevGain(bars, event_queue, perc=4),
#     OneSidedOrderOnly(bars, event_queue, OrderPosition.BUY)
# ])

port = PercentagePortFolio(bars, event_queue, order_queue,
                           percentage=0.20,
                           portfolio_name=(
                               args.name if args.name != "" else "loop"),
                           mode='asset',
                           expires=7,
                           rebalance=SellLongLosersYearly,
                           portfolio_strategy=SellLowestPerforming
                           )
broker = SimulatedBroker(bars, port, event_queue, order_queue)
backtest(bars, event_queue, order_queue,
         strategy, port, broker, start_date=start_date)
