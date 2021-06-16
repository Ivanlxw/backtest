import json
import os
import argparse
import time
import queue
import logging
from backtest.Plots.plot import Plot
import pandas as pd

from backtest.utilities.constants import backtest_basepath

NY = "America/New_York"


def remove_bs(s: str):
    # remove backslash at the end from reading from a stock_list.txt
    return s.replace("\n", "")


def load_credentials(credentials_fp):
    with open(credentials_fp, "r") as f:
        credentials = json.load(f)
        for k, v in credentials.items():
            os.environ[k] = v


def parse_args():
    parser = argparse.ArgumentParser(description="Configs for running main.")
    parser.add_argument(
        "-c", "--credentials", required=True, type=str, help="credentials filepath"
    )
    parser.add_argument(
        "-n",
        "--name",
        required=False,
        default="",
        type=str,
        help="name of backtest/live strat run",
    )
    parser.add_argument(
        "-b",
        "--backtest",
        required=False,
        type=bool,
        default=True,
        help="backtest filters?",
    )
    parser.add_argument(
        "-f",
        "--fundamental",
        required=False,
        type=bool,
        default=False,
        help="Use fundamental data or not",
    )
    parser.add_argument(
        "--data_dir",
        default="./data/daily",
        required=False,
        type=str,
        help="filepath to dir of csv files",
    )
    return parser.parse_args()


def _backtest_loop(bars, event_queue, order_queue, strategy, port, broker, loop_live: bool = False) -> Plot:
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
                        port.update_timeindex(event)
                        if (signal_list := strategy.calculate_signals(event)) is not None:
                            for signal in signal_list:
                                event_queue.put(signal)
                        while not order_queue.empty():
                            event_queue.put(order_queue.get())

                    elif event.type == 'SIGNAL':
                        port.update_signal(event)

                    elif event.type == 'ORDER':
                        if broker.execute_order(event):
                            logging.info(event.print_order())

                    elif event.type == 'FILL':
                        port.update_fill(event)

    print(f"Backtest finished in {time.time() - start}. Getting summary stats")
    port.create_equity_curve_df()
    logging.log(32, port.output_summary_stats())

    plotter = Plot(port)
    plotter.plot()
    return plotter


def _life_loop(bars, event_queue, order_queue, strategy, port, broker) -> Plot:
    start = time.time()
    while True:
        # Update the bars (specific backtest code, as opposed to live trading)
        now = pd.Timestamp.now(tz=NY)
        if now.hour == 9 and now.minute >= 45:
            # Update the bars (specific backtest code, as opposed to live trading)
            bars.update_bars()
            if bars.start_date.dayofweek > 4:
                time.sleep(23 * 60 * 60)
            while True:
                try:
                    event = event_queue.get(block=False)
                except queue.Empty:
                    break
                else:
                    if event is not None:
                        if event.type == 'MARKET':
                            logging.info(f"{now}: MarketEvent")
                            port.update_timeindex(event)
                            if (signal_list := strategy.calculate_signals(event)) is not None:
                                for signal in signal_list:
                                    event_queue.put(signal)
                            while not order_queue.empty():
                                event_queue.put(order_queue.get())
                            # not sync w alpaca, only reflect current strat
                            logging.info(f"{bars.all_holdings[-1]}")

                        elif event.type == 'SIGNAL':
                            port.update_signal(event)

                        elif event.type == 'ORDER':
                            if broker.execute_order(event):
                                logging.info(event.print_order())

                        elif event.type == 'FILL':
                            port.update_fill(event)

            time.sleep(23 * 60 * 60)  # 23 hrs
