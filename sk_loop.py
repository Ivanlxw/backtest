"""
Actual file to run for backtesting 
"""
import time, datetime
import queue
import matplotlib.pyplot as plt

from backtest import utils, execution
from backtest.data.dataHandler import HistoricCSVDataHandler
from backtest.portfolio.base import NaivePortfolio, PercentagePortFolio
from backtest.data.FullData import CSVDataCreater
from backtest.strategy.sk.data import BaseSkData
from backtest.strategy.sk.strategy import SKRCStrategy

## sklearn modules
from sklearn.linear_model import LinearRegression

with open("data/stock_list.txt", 'r') as fin:
    stock_list = fin.readlines()

stock_list = list(map(utils.remove_bs, stock_list))

event_queue = queue.LifoQueue()
start_date = "2000-01-25"  ## YYYY-MM-DD
end_train_date = "2010-12-31"

## type: dict(pd.DataFrame)
train = CSVDataCreater(csv_dir="data/data/daily",
                    symbol_list=["AXP", "JNJ", "VZ","MSFT", "AMZN", "XOM", "PG"],
                    start_date=start_date,
                    end_date=end_train_date).get_data()

# Declare the components with respective parameters
## bars_test dates should not overlap with bars_train
bars = HistoricCSVDataHandler(event_queue, csv_dir="data/data/daily",
                                           symbol_list=["MMM", "JNJ", "VZ","MSFT", "MRK", "XOM", "PG"],
                                           start_date=end_train_date,
                                           )
regressor = LinearRegression()
strategy = SKRCStrategy(bars, event_queue, regressor, processor=BaseSkData(train, 14))
port = PercentagePortFolio(bars, event_queue, percentage=0.05)
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
plt.plot(port.equity_curve['equity_curve'])
plt.plot(port.equity_curve['liquidity_curve'])
plt.subplot(2,1,2)
plt.title("Assets over time")
plt.plot(port.equity_curve["total"])
plt.plot(port.equity_curve['cash'])
plt.tight_layout()
plt.show()