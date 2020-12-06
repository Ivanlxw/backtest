"""
Actual file to run for backtesting 
"""
import time
import queue
import matplotlib.pyplot as plt
import random
from pandas.io.formats import style
import talib 

from backtest import utils, execution
from backtest.data.dataHandler import HistoricCSVDataHandler
from backtest.portfolio.base import NaivePortfolio, PercentagePortFolio
from backtest.strategy.ma import SimpleCrossStrategy, DoubleMAStrategy, MeanReversionTA
from backtest.benchmark.benchmark import plot_benchmark

with open("data/stock_list.txt", 'r') as fin:
    stock_list = fin.readlines()

stock_list = list(map(utils.remove_bs, stock_list))

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
start_date = "2015-01-05"  ## YYYY-MM-DD
# start_date = "2019-12-03"  ## YYYY-MM-DD
symbol_list = random.sample(stock_list, 10)

# Declare the components with relsspective parameters
bars = HistoricCSVDataHandler(event_queue, csv_dir="data/data/daily",
                                           symbol_list=symbol_list, 
                                           start_date=start_date,
                                           )
strategy = DoubleMAStrategy(bars, event_queue, [14,50], talib.EMA)                                       
# strategy = MeanReversionTA(bars, event_queue, 50, talib.SMA, sd=2.5, exit=True)
# strategy = SimpleCrossStrategy(bars, event_queue, 50, talib.SMA)
port = PercentagePortFolio(bars, event_queue, order_queue, percentage=1/len(symbol_list), mode='asset')
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
plt.subplot(2,1,1)
plt.title("Equity curve")
plt.plot(port.equity_curve['equity_curve'], label="strat_eq")
plt.plot(port.equity_curve['liquidity_curve'], label="strat_cash")
plt.subplot(2,1,2)
plt.title("Assets over time")
plt.plot(port.equity_curve["total"], label="strat_total")
plt.plot(port.equity_curve['cash'], label="strat_cash")
plt.tight_layout()

plot_benchmark("data/stock_list.txt", \
    symbol_list=symbol_list, \
    start_date = start_date)

plt.legend()
plt.show()