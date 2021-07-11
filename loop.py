import queue
import os
import random
import logging
import os
from trading.strategy.basic import BoundedPercChange, OneSidedOrderOnly
from trading.utilities.enum import OrderPosition
import talib
import pandas as pd

from backtest.utilities.utils import generate_start_date, parse_args, remove_bs, load_credentials
from backtest.broker import SimulatedBroker, TDABroker
from backtest.portfolio.rebalance import BaseRebalance, SellLongLosersQuarterly, SellLongLosersYearly
from backtest.portfolio.portfolio import PercentagePortFolio
from backtest.portfolio.strategy import DefaultOrder, LongOnly, SellLowestPerforming
from backtest.utilities.backtest import backtest
from backtest.strategy.fundamental import HighRevGain, LowDCF
from trading.data.dataHandler import FMPData, HistoricCSVDataHandler, TDAData
from trading.strategy.multiple import MultipleAllStrategy
from trading.strategy.ta import BoundedTA, ExtremaTA, MeanReversionTA, TAIndicatorType
from trading.strategy.statistics import ExtremaBounce, LongTermCorrTrend, RelativeExtrema

args = parse_args()

if args.name != "":
    logging.basicConfig(filename=args.name+'.log', level=logging.INFO)

with open(f"{os.path.abspath(os.path.dirname(__file__))}/data/snp500.txt", 'r') as fin:
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
# csv_dir = os.path.abspath(
#     os.path.dirname(__file__))+"/data/data/daily"
# symbol_list = list(set(
#     random.sample([
#         fn.replace('.csv', '') for fn in os.listdir(csv_dir)
#     ], 50) + ["DUK", "AON", "C", "UAL", "AMZN", "COG"]))

symbol_list = list(set(
    random.sample(symbol_list, 50) + ["DUK", "AON", "C", "UAL", "AMZN", "COG"]))

# bars = HistoricCSVDataHandler(event_queue,
#                               csv_dir,
#                               symbol_list,
#                               start_date=start_date,
#                               end_date=end_date
#                               )

bars = FMPData(event_queue, symbol_list, start_date,
               frequency_type="daily", end_date=end_date)

strategy = MultipleAllStrategy([
    RelativeExtrema(bars, event_queue,
                    long_time=30,
                    percentile=10, strat_contrarian=True),
    BoundedTA(bars, event_queue, 7, 20, floor=30, ceiling=70,
              ta_indicator=talib.RSI, ta_indicator_type=TAIndicatorType.TwoArgs),
    BoundedTA(bars, event_queue, 7, 20, floor=-100, ceiling=100,
              ta_indicator=talib.CCI, ta_indicator_type=TAIndicatorType.ThreeArgs),
    OneSidedOrderOnly(bars, event_queue, OrderPosition.BUY)
])

MultipleAllStrategy([
    OneSidedOrderOnly(bars, event_queue, OrderPosition.BUY),
    # LowDCF(bars, event_queue, buy_ratio=1.25, sell_ratio=3.2),
    HighRevGain(bars, event_queue, perc=5),
    RelativeExtrema(bars, event_queue,
                    long_time=100,
                    percentile=5, strat_contrarian=True),
    BoundedTA(bars, event_queue, 7, 20, floor=30, ceiling=70,
              ta_indicator=talib.RSI, ta_indicator_type=TAIndicatorType.TwoArgs)
    # ExtremaBounce(bars, event_queue, 7, 50, percentile=25),
])

# strategy = MultipleAllStrategy([
#     ExtremaBounce(bars, event_queue, 10, 100, percentile=15),
#     MeanReversionTA(bars, event_queue, 20, talib.SMA, exit=False),
#     ExtremaTA(bars, event_queue, talib.RSI, 14, TAIndicatorType.TwoArgs,
#               7, strat_contrarian=False, consecutive=1),
# ])

port = PercentagePortFolio(bars, event_queue, order_queue,
                           percentage=0.10,
                           portfolio_name=(
                               args.name if args.name != "" else "loop"),
                           mode='asset',
                           expires=7,
                           rebalance=SellLongLosersQuarterly,
                           portfolio_strategy=SellLowestPerforming
                           )
broker = SimulatedBroker(bars, port, event_queue, order_queue)
backtest(symbol_list, bars, event_queue, order_queue,
         strategy, port, broker, start_date=start_date)
