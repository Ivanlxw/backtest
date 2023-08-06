from collections import deque
import datetime
import logging
import time
import queue
from typing import List

import pandas as pd

from trading.broker.broker import Broker
from trading.event import SignalEvent
from trading.portfolio.portfolio import Portfolio
from backtest.utilities.utils import NY_TIMEZONE, get_sleep_time, log_message
from trading.strategy.base import Strategy
from trading.data.dataHandler import DataHandler
from trading.plots.plot import Plot


class Backtest:
    def __init__(self, data_provider, strategy, portfolio, broker, args) -> None:
        self.data_provider: DataHandler = data_provider
        self.strategy: Strategy = strategy
        self.portfolio: Portfolio = portfolio
        self.broker: Broker = broker
        self.event_queue = deque([])
        self.order_queue = queue.Queue()

        self.show_plot = args.num_runs == 1
        self.sleep_duration = get_sleep_time(args.frequency)

        self.portfolio.Initialize(
            self.data_provider.symbol_list,
            self.data_provider.start_ms,
            self.data_provider.option_metadata_info,
        )

    def run(self, live: bool):
        # core backtest logic
        if live:
            self._life_loop()
        else:
            plotter = self._backtest_loop()

    def _backtest_loop(self):
        start = time.time()
        while True:
            # Update the bars (specific backtest code, as opposed to live trading)
            if self.data_provider.continue_backtest == True:
                self.data_provider.update_bars(self.event_queue)
            else:
                while len(self.event_queue) > 0:
                    self.event_queue.pop()
                break
            while True:
                try:
                    event = self.event_queue.pop()
                except IndexError:
                    break
                else:
                    if event is not None:
                        if event.type == "MARKET":
                            market_bar = self.data_provider.get_latest_bars(event.symbol, 2)
                            self.portfolio.update_option_datetime(market_bar, self.event_queue)
                            self.portfolio.update_timeindex(market_bar, self.event_queue)
                            signal_list = self.strategy.calculate_signals(event, curr_holdings=self.portfolio.current_holdings)
                            for signal in signal_list:
                                self.event_queue.appendleft(signal)
                            while not self.order_queue.empty():
                                self.event_queue.appendleft(self.order_queue.get())

                        elif event.type == "SIGNAL":
                            self.portfolio.update_signal(
                                event, self.event_queue
                            )  # sends OrderEvent

                        elif event.type == "ORDER":
                            if self.broker.execute_order(
                                event, self.event_queue, self.order_queue
                            ):
                                log_message(event.details())

                        elif event.type == "FILL":
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
            if time_since_midnight < datetime.timedelta(
                hours=9, minutes=45
            ) or time_since_midnight > datetime.timedelta(hours=17, minutes=45):
                continue
            self.data_provider.update_bars(self.event_queue, live=True)
            while True:
                try:
                    event = self.event_queue.get(block=False)
                except queue.Empty:
                    break
                else:
                    if event is not None:
                        if event.type == "MARKET":
                            log_message("MarketEvent")
                            market_bar = self.data_provider.get_latest_bars(event.symbol, 2)
                            self.portfolio.update_option_datetime(market_bar, self.event_queue)
                            self.portfolio.update_timeindex(market_bar, self.event_queue)
                            self.broker.update_portfolio_positions()
                            signal_list = self.strategy.calculate_signals(event)
                            for signal in signal_list:
                                self.event_queue.appendleft(signal)
                            while not self.order_queue.empty():
                                self.event_queue.appendleft(self.order_queue.get())

                        elif event.type == "SIGNAL":
                            self.portfolio.update_signal(event, self.event_queue)

                        elif event.type == "ORDER":
                            if self.broker.execute_order(
                                event, self.event_queue, self.order_queue
                            ):
                                logging.info(event.order_details())

                        elif event.type == "FILL":
                            self.portfolio.update_fill(event, True)
                            self.broker.update_portfolio_positions()
                            self.portfolio.write_curr_holdings()
            # end of cycle
            self.portfolio.write_curr_holdings()
            log_message("sleeping")
            time.sleep(self.sleep_duration.total_seconds())
            log_message("sleep over")
