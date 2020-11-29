"""
Actual file to run for backtesting 
"""
import os
import time, datetime
import queue
import matplotlib.pyplot as plt

from backtest import execution
from backtest.data.dataHandler import HistoricCSVDataHandler
from backtest.portfolio.base import NaivePortfolio, PercentagePortFolio
from backtest.strategy.naive import BuyAndHoldStrategy

def plot_benchmark(stock_list_fp, symbol_list, start_date, freq="daily"):
    event_queue = queue.LifoQueue()
    # Declare the components with relsspective parameters
    csv_dir = os.path.dirname(os.getcwd() + "/" +stock_list_fp) + f"/data/{freq}" 
    bars = HistoricCSVDataHandler(event_queue, csv_dir=csv_dir,
                                            symbol_list=symbol_list,
                                            start_date=start_date)
    strategy = BuyAndHoldStrategy(bars, event_queue)
    port = PercentagePortFolio(bars, event_queue, percentage=1/len(symbol_list), mode='asset')
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
                        strategy.calculate_signals(event)
                        port.update_timeindex(event)

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
    plt.plot(port.equity_curve['equity_curve'], label="benchmark_eq")
    plt.plot(port.equity_curve['liquidity_curve'], label="benchmark_cash")
    plt.subplot(2,1,2)
    plt.title("Assets over time")
    plt.plot(port.equity_curve["total"], label="benchmark_total")
    plt.plot(port.equity_curve['cash'], label="benchmark_cash")
    plt.tight_layout()