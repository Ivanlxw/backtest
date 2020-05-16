"""
Actual file to run for backtesting 
"""
import time
import queue
from backtest import portfolio, data_handler, strategy, utils, execution

with open("data/stock_list.txt", 'r') as fin:
    stock_list = fin.readlines()

stock_list = list(map(utils.remove_bs, stock_list))

event_queue = queue.Queue()

# Declare the components with relsspective parameters
bars = data_handler.HistoricCSVDataHandler(event_queue, csv_dir="data/daily",
                                           symbol_list=["GS", "WMT", "BAC","MSFT"])
strategy = strategy.BuyAndHoldStrategy(bars, event_queue)
port = portfolio.NaivePortfolio(bars, event_queue, start_date="2000-01-30")
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
                    strategy.calculate_signals(event)
                    port.update_timeindex(event)

                elif event.type == 'SIGNAL':
                    port.update_signal(event, 100) ## order 100 stocks everytime

                elif event.type == 'ORDER':
                    broker.execute_order(event)

                elif event.type == 'FILL':
                    port.update_fill(event)

    # 10-Minute heartbeat
    # time.sleep(10*60)
port.create_equity_curve_df()
print(port.equity_curve)
print(port.output_summary_stats())
    