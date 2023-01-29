import datetime
import logging
import time
import queue
import copy
from typing import List

import pandas as pd
import matplotlib.pyplot as plt

from trading.broker.broker import Broker, SimulatedBroker
from trading.event import SignalEvent
from trading.portfolio.portfolio import PercentagePortFolio, Portfolio
from trading.strategy.base import BuyAndHoldStrategy
from backtest.utilities.utils import NY_TIMEZONE, log_message
from trading.data.dataHandler import DataHandler, HistoricCSVDataHandler
from trading.utilities.enum import OrderType
from trading.plots.plot import Plot

BENCHMARK_TICKER = "SPY"


def _backtest_loop(bars, event_queue, order_queue, strategy, port, broker) -> Plot:
    start = time.time()
    while True:
        # Update the bars (specific backtest code, as opposed to live trading)
        if bars.continue_backtest == True:
            bars.update_bars()
        else:
            while not event_queue.empty():
                event_queue.get()
            break
        while True:
            try:
                event = event_queue.get(block=False)
            except queue.Empty:
                break
            else:
                if event is not None:
                    if event.type == 'MARKET':
                        port.update_timeindex()
                        signal_list: List[SignalEvent] = strategy.calculate_signals(
                            event)
                        for signal in signal_list:
                            event_queue.put(signal)
                        while not order_queue.empty():
                            event_queue.put(order_queue.get())

                    elif event.type == 'SIGNAL':
                        port.update_signal(event)   # sends OrderEvent

                    elif event.type == 'ORDER':
                        if broker.execute_order(event):
                            log_message(event.order_details())

                    elif event.type == 'FILL':
                        port.update_fill(event)

    print(f"Backtest finished in {time.time() - start}. Getting summary stats")
    port.create_equity_curve_df()
    log_message(port.output_summary_stats())
    plotter = Plot(port)
    plotter.plot()
    return plotter


def _life_loop(bars, event_queue, order_queue, strategy, port: Portfolio, broker: Broker, sleep_duration: datetime.timedelta) -> Plot:
    while True:
        # Update the bars (specific backtest code, as opposed to live trading)
        now = pd.Timestamp.now(tz=NY_TIMEZONE)
        if now.dayofweek >= 4 and now.hour > 17:
            break
        time_since_midnight = now - now.normalize()
        # only run during trading hours -> 0945 - 1745
        if (time_since_midnight < datetime.timedelta(hours=9, minutes=45) or time_since_midnight > datetime.timedelta(hours=17, minutes=45)):
            continue
        bars.update_bars()
        while True:
            try:
                event = event_queue.get(block=False)
            except queue.Empty:
                break
            else:
                if event is not None:
                    if event.type == 'MARKET':
                        log_message("MarketEvent")
                        port.update_timeindex()
                        broker.update_portfolio_positions()
                        signal_list = strategy.calculate_signals(event)
                        for signal in signal_list:
                            event_queue.put(signal)
                        while not order_queue.empty():
                            event_queue.put(order_queue.get())

                    elif event.type == 'SIGNAL':
                        port.update_signal(event)

                    elif event.type == 'ORDER':
                        if broker.execute_order(event):
                            logging.info(event.order_details())

                    elif event.type == 'FILL':
                        port.update_fill(event)
                        broker.update_portfolio_positions()
                        port.write_curr_holdings()
        # end of cycle
        port.write_curr_holdings()
        log_message("sleeping")
        time.sleep(sleep_duration.total_seconds())
        log_message("sleep over")


def backtest(bars: DataHandler, creds: dict, event_queue, order_queue,
             strategy, port, broker, frequency,
             loop_live: bool = False,
             show_plot: bool = True,
             sleep_duration: int = 86400, initial_capital=100000):
    if loop_live:
        _life_loop(bars, event_queue, order_queue, strategy, port,
                   broker, datetime.timedelta(seconds=sleep_duration))
    else:
        _backtest_loop(bars, event_queue, order_queue, strategy, port, broker)
        plot_benchmark(creds, symbol_list=bars.symbol_data.keys(), portfolio_name="benchmark_BuyAndHold", freq=frequency,
                       benchmark_bars=copy.copy(bars), initial_capital=initial_capital)
        plot_benchmark(creds, symbol_list=[BENCHMARK_TICKER], portfolio_name="benchmark_index",
                       freq=frequency, start_ms=bars.start_ms, end_ms=bars.end_ms, initial_capital=initial_capital)
        if show_plot:
            plt.legend()
            plt.show()


def plot_benchmark(creds, symbol_list, portfolio_name, freq: str, benchmark_bars=None, start_ms: int = None,  end_ms: int = None, initial_capital=100000):
    event_queue = queue.LifoQueue()
    order_queue = queue.Queue()
    if benchmark_bars is None and start_ms is None:
        raise Exception("If benchmark_bars is None, start_ms cannot be None")
    elif benchmark_bars is None and start_ms is not None:
        # create new benchmark_bars with index only
        benchmark_bars = HistoricCSVDataHandler(event_queue,
                                                symbol_list=symbol_list,
                                                creds=creds,
                                                start_ms=start_ms,
                                                end_ms=end_ms,
                                                frequency_type=freq
                                                )
    benchmark_bars.events = event_queue
    # Declare the components with relsspective parameters
    strategy = BuyAndHoldStrategy(benchmark_bars, event_queue)
    port = PercentagePortFolio(benchmark_bars, event_queue, order_queue, initial_capital=initial_capital,
                               percentage=1/len(symbol_list), portfolio_name=portfolio_name,
                               mode='asset', order_type=OrderType.MARKET)
    broker = SimulatedBroker(benchmark_bars, port, event_queue, order_queue)

    _backtest_loop(benchmark_bars, event_queue,
                   order_queue, strategy, port, broker)
