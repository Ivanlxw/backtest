import json
import datetime
import argparse
from pathlib import Path
import time
import queue
import logging
import os
import random
import pandas as pd
from trading.plots.plot import Plot
from trading.utilities.constants import backtest_basepath

NY = "America/New_York"
UTILS_ABS_FP = Path(os.path.dirname(os.path.abspath(__file__)))
MODELINFO_DIR = UTILS_ABS_FP / "../../Data/strategies"
if not os.path.exists(MODELINFO_DIR):
    os.makedirs(MODELINFO_DIR, exist_ok=True)


def parse_args():
    parser = argparse.ArgumentParser(description='Configs for running main.')
    parser.add_argument('-c', '--credentials', required=True,
                        type=str, help="credentials filepath")
    parser.add_argument('-n', '--name', required=False, default="",
                        type=str, help="name of backtest/live strat run")
    parser.add_argument('-l', '--live', required=False, type=bool, default=False,
                        help='inform life?')
    parser.add_argument("--num-runs", type=int, default=1, help="Run backtest x times, get more aggregated performance details from log")
    parser.add_argument("--frequency", type=str, default="daily", help="Frequency of data. Searches a dir with same name")
    parser.add_argument("--sleep-time", type=int, default=43200, help="Sleep time in seconds")
    return parser.parse_args()


def remove_bs(s: str):
    # remove backslash at the end from reading from a stock_list.txt
    return s.replace("\n", "")


def load_credentials(credentials_fp):
    with open(credentials_fp, 'r') as f:
        credentials = json.load(f)
        for k, v in credentials.items():
            os.environ[k] = v


def generate_start_date():
    return "{}-{:02d}-{:02d}".format(
        random.randint(2010, 2018),
        random.randint(1, 12),
        random.randint(1, 28)
    )


def log_message(message: str):
    logging.info(f"{pd.Timestamp.now()}: {message}")

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
                        signal_list = strategy.calculate_signals(event)
                        for signal in signal_list:
                            if signal is not None:
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

    print(f"Backtest finished in {time.time() - start}. Getting summary stats")
    port.create_equity_curve_df()
    logging.log(32, port.output_summary_stats())
    plotter = Plot(port)
    plotter.plot()
    return plotter


def _life_loop(bars, event_queue, order_queue, strategy, port, broker, sleep_duration: datetime.timedelta) -> Plot:
    while True:
        # Update the bars (specific backtest code, as opposed to live trading)
        now = pd.Timestamp.now(tz=NY)
        if now.dayofweek >= 4 and now.hour > 17:
            break
        if not ((now.hour >= 9 and now.minute > 45) and now.hour < 18):  # only run during trading hours -> 0945 - 1805
            continue
        log_message("update_bars()")
        bars.update_bars()
        while True:
            try:
                event = event_queue.get(block=False)
            except queue.Empty:
                break
            else:
                if event is not None:
                    if event.type == 'MARKET':
                        logging.info(f"{now}: MarketEvent")
                        port.update_timeindex()
                        signal_list = strategy.calculate_signals(event)
                        for signal in signal_list:
                            if signal is not None:
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
        log_message("sleeping")
        time.sleep(sleep_duration.total_seconds())  # 18 hrs
        log_message("sleep over")
