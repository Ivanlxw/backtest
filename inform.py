import argparse
import json
import logging
import os
import queue
import time
from trading''.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy

import talib
import pandas as pd

from trading''.data.dataHandler import HistoricCSVDataHandler, NY, TDAData
from Inform.filter import FundamentalFilter
from Inform.telegram.inform import telegram_bot_sendtext
from trading''.strategy.statistics import ExtremaBounce, LongTermCorrTrend
from trading''.strategy.ta import BoundedTA, ExtremaTA, MeanReversionTA, TAIndicatorType
from trading''.plots.plot import PlotIndividual


def parse_args():
    parser = argparse.ArgumentParser(description='Configs for running main.')
    parser.add_argument('-l', '--live', required=False, type=bool, default=False,
                        help='inform life?')
    parser.add_argument('-c', '--credentials', required=True,
                        type=str, help="credentials filepath")
    parser.add_argument('--data_dir', default="./data/daily",
                        required=False, type=str, help="filepath to dir of csv files")
    return parser.parse_args()


args = parse_args()
with open("./data/snp500.txt", 'r') as fin:
    stock_list = fin.readlines()
stock_list = list(map(lambda x: x.replace('\n', ''), stock_list))
symbol_list = stock_list

with open(args.credentials, 'r') as f:
    credentials = json.load(f)
    for k, v in credentials.items():
        os.environ[k] = v

event_queue = queue.LifoQueue()
start_date = '2015-01-07'
if not args.live:
    import random
    csv_dir = os.path.abspath(os.path.abspath(
        os.path.dirname(__file__)) + "/data/data/daily")
    bars = HistoricCSVDataHandler(event_queue,
                                  csv_dir,
                                  random.sample([
                                      fn.replace('.csv', '') for fn in os.listdir(csv_dir)
                                  ], 100),
                                  start_date=start_date, fundamental=False
                                  )
else:
    bars = TDAData(event_queue, symbol_list, start_date, live=True)

filter = MultipleAllStrategy([
    ExtremaBounce(bars, 7, 60),
    # ExtremaTA(bars, event_queue, talib.RSI, 14,
    #           TAIndicatorType.TwoArgs,
    #           extrema_period=10, strat_contrarian=True,
    #           consecutive=2),
    # BoundedTA(bars, event_queue, 7, 14, floor=32, ceiling=70,
    #           ta_indicator=talib.RSI, ta_indicator_type=TAIndicatorType.TwoArgs),
    LongTermCorrTrend(bars, event_queue, 120)
])

signals = queue.Queue()
start = time.time()
while True:
    now = pd.Timestamp.now(tz=NY)
    if args.live and not (now.hour == 9 and now.minute > 35 and now.dayofweek <= 4):
        continue
    if bars.continue_backtest == True:
        bars.update_bars()  # will take about 550s
    else:
        break

    if not event_queue.empty():
        event = event_queue.get(block=False)
        if event.type == 'MARKET':
            if args.live:
                logging.log(
                    level=35, msg=f"{pd.Timestamp.now(tz=NY)}: MarketEvent")
            signal_events = filter.calculate_signals(event)
            for signal_event in signal_events:
                if signal_event is not None:
                    signals.put(signal_event)
    if args.live:
        while not signals.empty():
            # TODO: send to phone via tele
            signal_event = signals.get(block=False)
            print(signal_event.details())
            res = telegram_bot_sendtext(signal_event.details(),
                                        os.environ["TELEGRAM_APIKEY"], os.environ["TELEGRAM_CHATID"])
        logging.log(level=35, msg=f"{pd.Timestamp.now(tz=NY)}: sleeping")
        time.sleep(23 * 60 * 60)

signals = list(signals.queue)
print(f"Event loop finished in {time.time() - start}s.\n\
    Number of signals: {len(signals)}")
plot = PlotIndividual(bars, signals)
plot.plot()
