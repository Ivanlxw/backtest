"""
Actual file to run for backtesting 
"""
import time
import queue
import random
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier

from backtest import utils, execution
from backtest.benchmark.benchmark import plot_benchmark
from backtest.data.dataHandler import HistoricCSVDataHandler
from backtest.portfolio.base import PercentagePortFolio
from backtest.portfolio.rebalance.base import BaseRebalance
from backtest.strategy.sk.data import ClassificationData
from backtest.strategy.sk.strategy import SKCStrategy

with open("data/stock_list.txt", 'r') as fin:
    stock_list = fin.readlines()

stock_list = list(map(utils.remove_bs, stock_list))

event_queue = queue.LifoQueue()
start_date = "2000-01-25"  ## YYYY-MM-DD
end_train_date = "2010-12-31"
symbol_list = random.sample(stock_list, 7)

start = time.time()
# Declare the components with respective parameters
## bars_test dates should not overlap with bars_train
bars = HistoricCSVDataHandler(event_queue, csv_dir="data/data/daily",
                                           symbol_list=symbol_list,
                                           start_date=end_train_date,
                                           )
train = HistoricCSVDataHandler(None, csv_dir="data/data/daily",
                                           symbol_list=symbol_list,
                                           start_date=start_date,
                                           end_date=end_train_date,
                                           datahandler=False
                                           )

clf = RandomForestClassifier()
strategy = SKCStrategy(bars, event_queue, clf, processor=ClassificationData(train, 14, 2))
port = PercentagePortFolio(bars, event_queue, percentage=0.05, rebalance=BaseRebalance(event_queue))
broker = execution.SimulatedExecutionHandler(event_queue)

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
    start_date = end_train_date)

plt.legend()
plt.show()