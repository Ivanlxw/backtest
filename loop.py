"""
Actual file to run for backtesting 
"""
import time, datetime
import queue
import matplotlib.pyplot as plt

from backtest import utils, execution
from backtest.data.dataHandler import HistoricCSVDataHandler
from backtest.portfolio.base import NaivePortfolio, PercentagePortFolio
from backtest.strategy.cross_strategy import SimpleCrossStrategy, MeanReversionTA
from backtest.strategy.naive import BuyAndHoldStrategy
from backtest.benchmark.benchmark import plot_benchmark

with open("data/stock_list.txt", 'r') as fin:
    stock_list = fin.readlines()

stock_list = list(map(utils.remove_bs, stock_list))

event_queue = queue.LifoQueue()
start_date = "2010-01-05"  ## YYYY-MM-DD

# Declare the components with relsspective parameters
bars = HistoricCSVDataHandler(event_queue, csv_dir="data/data/daily",
                                           symbol_list=["GS", "WMT", "BAC","MSFT", "AMZN", "VZ", "PG"],
                                           start_date=start_date,
                                           end_date="2016-12-01")
strategy = MeanReversionTA(bars, event_queue, cross_type="sma", timeperiod=50, sd=1.5, exit="cross")
port = PercentagePortFolio(bars, event_queue, percentage=0.10)
broker = execution.SimulatedExecutionHandler(event_queue)

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
    symbol_list=stock_list, \
    start_date = start_date)

plt.legend()
plt.show()