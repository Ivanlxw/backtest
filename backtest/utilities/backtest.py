import datetime
import logging
import time
import queue
from typing import List

import pandas as pd
import matplotlib.pyplot as plt

from trading.broker.broker import Broker 
from trading.event import SignalEvent
from trading.portfolio.portfolio import Portfolio
from backtest.utilities.utils import NY_TIMEZONE, get_sleep_time, log_message
from trading.strategy.base import Strategy
from trading.data.dataHandler import DataHandler
from trading.plots.plot import Plot

class Backtest:
    def __init__(self, data_provider, strategy, portfolio, broker, args) -> None:
        self.data_provider: DataHandler  = data_provider
        self.strategy: Strategy = strategy
        self.portfolio: Portfolio = portfolio
        self.broker: Broker = broker
        self.event_queue = queue.LifoQueue()
        self.order_queue = queue.Queue()
        
        self.show_plot = args.num_runs == 1
        self.sleep_duration = get_sleep_time(args.frequency)

        self.portfolio.Initialize(self.data_provider.symbol_data.keys(), self.data_provider.start_ms)

    def run(self, live: bool):
        # core backtest logic
        if live:
            self._life_loop()
        else:
            plotter = self._backtest_loop()
            # plot_benchmark(creds, symbol_list=bars.symbol_data.keys(), portfolio_name="benchmark_BuyAndHold", freq=frequency,
            #     benchmark_bars=copy.copy(bars), initial_capital=initial_capital)
            # plot_benchmark(creds, symbol_list=[BENCHMARK_TICKER], portfolio_name="benchmark_index",
            #             freq=frequency, start_ms=bars.start_ms, end_ms=bars.end_ms, initial_capital=initial_capital)


    def _backtest_loop(self):
        start = time.time()
        while True:
            # Update the bars (specific backtest code, as opposed to live trading)
            if self.data_provider.continue_backtest == True:
                self.data_provider.update_bars(self.event_queue)
            else:
                while not self.event_queue.empty():
                    self.event_queue.get()
                break
            while True:
                try:
                    event = self.event_queue.get(block=False)
                except queue.Empty:
                    break
                else:
                    if event is not None:
                        if event.type == 'MARKET':
                            self.portfolio.update_timeindex(self.data_provider, self.event_queue)
                            signal_list: List[SignalEvent] = self.strategy.calculate_signals(
                                event)
                            for signal in signal_list:
                                self.event_queue.put(signal)
                            while not self.order_queue.empty():
                                self.event_queue.put(self.order_queue.get())

                        elif event.type == 'SIGNAL':
                            self.portfolio.update_signal(event, self.event_queue)   # sends OrderEvent

                        elif event.type == 'ORDER':
                            if self.broker.execute_order(event, self.event_queue, self.order_queue):
                                log_message(event.order_details())

                        elif event.type == 'FILL':
                            self.portfolio.update_fill(event, False)

        print(f"Backtest finished in {time.time() - start}. Getting summary stats")
        self.portfolio.create_equity_curve_df()
        log_message(self.portfolio.output_summary_stats())
        plotter = Plot(self.portfolio)
        plotter.plot()
        return plotter

    
    def _life_loop(self) -> None:
        while True:
            # Update the bars (specific backtest code, as opposed to live trading)
            now = pd.Timestamp.now(tz=NY_TIMEZONE)
            if (now.dayofweek == 4 and now.hour > 17) or now.dayofweek > 4:
                break
            time_since_midnight = now - now.normalize()
            # only run during trading hours -> 0945 - 1745
            if (time_since_midnight < datetime.timedelta(hours=9, minutes=45) or time_since_midnight > datetime.timedelta(hours=17, minutes=45)):
                continue
            self.data_provider.update_bars(self.event_queue, self.event_queue, live=True)
            while True:
                try:
                    event = self.event_queue.get(block=False)
                except queue.Empty:
                    break
                else:
                    if event is not None:
                        if event.type == 'MARKET':
                            log_message("MarketEvent")
                            self.portfolio.update_timeindex(self.data_provider)
                            self.broker.update_portfolio_positions()
                            signal_list = self.strategy.calculate_signals(event)
                            for signal in signal_list:
                                self.event_queue.put(signal)
                            while not self.order_queue.empty():
                                self.event_queue.put(self.order_queue.get())

                        elif event.type == 'SIGNAL':
                            self.portfolio.update_signal(event, self.event_queue)

                        elif event.type == 'ORDER':
                            if self.broker.execute_order(event, self.event_queue, self.order_queue):
                                logging.info(event.order_details())

                        elif event.type == 'FILL':
                            self.portfolio.update_fill(event, True)
                            self.broker.update_portfolio_positions()
                            self.portfolio.write_curr_holdings()
            # end of cycle
            self.portfolio.write_curr_holdings()
            log_message("sleeping")
            time.sleep(self.sleep_duration.total_seconds())
            log_message("sleep over")
