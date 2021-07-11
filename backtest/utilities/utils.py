import json
import argparse
import time
import queue
import logging
import os
import random
import pandas as pd
from trading.plots.plot import Plot
from trading.utilities.constants import backtest_basepath

NY = "America/New_York"


def parse_args():
    parser = argparse.ArgumentParser(description='Configs for running main.')
    parser.add_argument('-c', '--credentials', required=True,
                        type=str, help="credentials filepath")
    parser.add_argument('-n', '--name', required=False, default="",
                        type=str, help="name of backtest/live strat run")
    parser.add_argument('-l', '--live', required=False, type=bool, default=False,
                        help='inform life?')
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


def _life_loop(bars, event_queue, order_queue, strategy, port, broker) -> Plot:
    while True:
        # Update the bars (specific backtest code, as opposed to live trading)
        now = pd.Timestamp.now(tz=NY)
        if now.minute == 45:
            logging.info(f"beginning of loop: {now}")
        if not (now.hour == 9 and now.minute == 35):  # only run @ 0935 NY timing
            continue
        if now.dayofweek > 4:
            # Update the bars (specific backtest code, as opposed to live trading)
            logging.info("saving info")
            port.create_equity_curve_df()
            results_dir = os.path.join(backtest_basepath, "results")
            if not os.path.exists(results_dir):
                os.mkdir(results_dir)

            port.equity_curve.to_csv(os.path.join(
                results_dir, f"{port.name}.json"))
            break

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
                        if (signal_list := strategy.calculate_signals(event)) is not None:
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

        # write the day's portfolio status before sleeping
        # logging.info(f"{pd.Timestamp.now(tz=NY)}: sleeping")
        # time.sleep(6 * 60 * 60)  # 18 hrs
        # logging.info("sleep done")
