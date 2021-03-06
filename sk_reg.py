"""
Actual file to run for backtesting 
"""
import time
import queue
import random
import matplotlib.pyplot as plt
import talib

from backtest import execution
from backtest.utilities.utils import remove_bs
from backtest.data.dataHandler import HistoricCSVDataHandler
from backtest.portfolio.base import PercentagePortFolio
from backtest.strategy.statistics.data import BaseStatisticalData
from backtest.strategy.statistics.strategy import RawRegression
from backtest.benchmark.benchmark import plot_benchmark

## sklearn modules
from sklearn.linear_model import LinearRegression

with open("data/stock_list.txt", 'r') as fin:
    stock_list = fin.readlines()

stock_list = list(map(remove_bs, stock_list))

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
start_date = "2010-01-04"  ## YYYY-MM-DD
symbol_list = random.sample(stock_list, 10)

start = time.time()
# Declare the components with respective parameters
## bars_test dates should not overlap with bars_train
bars = HistoricCSVDataHandler(event_queue, csv_dir="data/data/daily",
                                           symbol_list=symbol_list,
                                           start_date=start_date,
                                           )

strategy = RawRegression(bars, event_queue, LinearRegression, BaseStatisticalData(bars, 30, 2, add_ta={
    'RSI': [talib.RSI, 14]
}), 100)
port = PercentagePortFolio(bars, event_queue, order_queue, percentage=0.01)
broker = execution.SimulatedExecutionHandler(bars, event_queue)

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

                elif event.type == 'OPTIMIZE':
                    strategy.optimize()

                elif event.type == 'SIGNAL':
                    port.update_signal(event)
                    # print(event.datetime)

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

plot_benchmark("data/stock_universe.txt", \
    symbol_list=symbol_list, \
    start_date = start_date)

plt.legend()
plt.show()

plotter.plot_trade_prices()