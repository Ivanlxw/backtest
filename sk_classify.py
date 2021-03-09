"""
Actual file to run for backtesting 
"""
import time
import queue
import random
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier

from backtest.broker import SimulatedBroker
from backtest.utilities.utils import load_credentials, parse_args, remove_bs
from backtest.benchmark.benchmark import plot_benchmark
from backtest.data.dataHandler import HistoricCSVDataHandler
from backtest.portfolio.base import PercentagePortFolio
from backtest.portfolio.rebalance.base import BaseRebalance
from backtest.strategy.stat_data import ClassificationData
from backtest.strategy.statistics import RawClassification

args = parse_args()

with open("data/stock_universe.txt", 'r') as fin:
    stock_list = fin.readlines()

load_credentials(args.credentials)
stock_list = list(map(remove_bs, stock_list))

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
start_date = "2000-01-25"  ## YYYY-MM-DD
symbol_list = random.sample(stock_list, 15)

start = time.time()
# Declare the components with respective parameters
## bars_test dates should not overlap with bars_train
bars = HistoricCSVDataHandler(event_queue, csv_dir="data/data/daily",
                                           symbol_list=symbol_list,
                                           start_date=start_date,
                                           end_date = "2010-12-31"
                                           )

strategy = RawClassification(bars, event_queue, RandomForestClassifier, processor=ClassificationData(bars, 14, 2), reoptimize_days=30)
port = PercentagePortFolio(bars, event_queue, order_queue, percentage=0.05, rebalance=BaseRebalance(event_queue))
broker = SimulatedBroker(bars, event_queue, order_queue)

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
                
                elif event.type == 'OPTIMIZE' and \
                    callable(strategy.optimize):
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
sns.set()
sns.set_style('darkgrid')
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
    start_date = start_date,
    end_date = "2010-12-31")

plt.legend()
plt.show()