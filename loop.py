"""
Actual file to run for backtesting 
"""
import time
import queue
import matplotlib.pyplot as plt
import random
import talib 
import os 
import json
import argparse

from backtest import execution
from backtest.utilities.utils import remove_bs
from backtest.data.dataHandler import HistoricCSVDataHandler
from backtest.portfolio.base import NaivePortfolio, PercentagePortFolio
from backtest.strategy.multiple import MultipleStrategy
from backtest.strategy.TA.ma import SimpleCrossStrategy, DoubleMAStrategy, MeanReversionTA
from backtest.strategy.fundamental import FundamentalFScoreStrategy
from backtest.benchmark.benchmark import plot_benchmark
from backtest.portfolio.strategy.base import LongOnly

def parse_args():
    parser = argparse.ArgumentParser(description='Configs for running main.')
    parser.add_argument('-b', '--backtest', required=False, type=bool, default=True,
                    help='backtest filters?')
    parser.add_argument('-c', '--credentials', required=False, type=str, help="credentials filepath")
    parser.add_argument('-f', '--fundamental', required=False, type=bool, default=False, help="Use fundamental data or not")
    parser.add_argument('--data_dir', default="./data/daily", required=False, type=str, help="filepath to dir of csv files")
    return parser.parse_args()
args = parse_args()

with open("data/dow_stock_list.txt", 'r') as fin:
    stock_list = fin.readlines()
stock_list = list(map(remove_bs, stock_list))

with open(args.credentials, 'r') as f:
    credentials = json.load(f)
    for k,v in credentials.items():
        os.environ[k]= v
            
event_queue = queue.LifoQueue()
order_queue = queue.Queue()
start_date = "2015-01-05"  ## YYYY-MM-DD
symbol_list = random.sample(stock_list, 8)

# Declare the components with relsspective parameters
bars = HistoricCSVDataHandler(event_queue, csv_dir="data/data/daily",
                                           symbol_list=symbol_list, 
                                           start_date=start_date,
                                           fundamental=args.fundamental
                                           )
strategy = MultipleStrategy([
    DoubleMAStrategy(bars, event_queue, [14,50], talib.EMA),
    SimpleCrossStrategy(bars, event_queue, 20, talib.SMA)
])
                                     
if args.fundamental:
    strategy = FundamentalFScoreStrategy(bars, event_queue)
port = PercentagePortFolio(bars, event_queue, order_queue, 
    percentage=0.05, 
    mode='asset',
    portfolio_strategy=LongOnly
)
broker = execution.SimulatedExecutionHandler(bars, event_queue)

start = time.time()
while True:
    # Update the bars (specific backtest code, as opposed to live trading)
    if bars.continue_backtest == True:
        bars.update_bars()
    else:
        break
    
    # Handle the events
    while True:
        try:
            event = event_queue.get(block=False)
        except queue.Empty:
            break
        else:
            if event is not None:
                if event.type == 'MARKET':
                    port.update_timeindex(event)
                    strategy.calculate_signals(event)
                    while not order_queue.empty():
                        event_queue.put(order_queue.get())

                elif event.type == 'SIGNAL':
                    port.update_signal(event)

                elif event.type == 'ORDER':
                    broker.execute_order(event)
                    # event.print_order()

                elif event.type == 'FILL':
                    port.update_fill(event)

    # 10-Minute heartbeat
    # time.sleep(10*60)
print(f"Backtest finished in {time.time() - start}. Getting summary stats")
port.create_equity_curve_df()
print(port.output_summary_stats())

from backtest.Plots.plot import PlotTradePrices
plotter = PlotTradePrices(port, bars)
plotter.plot()

plot_benchmark("data/stock_list.txt", \
    symbol_list=symbol_list, \
    start_date = start_date)

plt.legend()
plt.show()

plotter.plot_trade_prices()
