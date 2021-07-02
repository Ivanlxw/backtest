import json
import logging
import os
import queue
import time
import random
from trading.utilities.enum import OrderPosition
from trading.strategy.naive import OneSidedOrderOnly, Strategy
from trading.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy

import talib
import pandas as pd

from backtest.utilities.utils import parse_args
from backtest.utilities.utils import generate_start_date
from trading.data.dataHandler import HistoricCSVDataHandler, NY, TDAData
from Inform.filter import FundamentalFilter
from Inform.telegram.inform import telegram_bot_sendtext
from trading.strategy.statistics import ExtremaBounce, LongTermCorrTrend, RelativeExtrema
from trading.strategy.ta import BoundedTA, ExtremaTA, MeanReversionTA, TAIndicatorType
from trading.plots.plot import PlotIndividual


args = parse_args()
if args.name != "":
    logging.basicConfig(filename=args.name+'.log', level=logging.INFO)
with open("./data/snp500.txt", 'r') as fin:
    stock_list = fin.readlines()
stock_list = list(map(lambda x: x.replace('\n', ''), stock_list))
symbol_list = stock_list

with open(args.credentials, 'r') as f:
    credentials = json.load(f)
    for k, v in credentials.items():
        os.environ[k] = v

event_queue = queue.LifoQueue()
start_date = generate_start_date()
while pd.Timestamp(start_date).dayofweek > 4:
    start_date = generate_start_date()
print(start_date)
end_date = "2020-01-30"
if not args.live:
    csv_dir = os.path.abspath(os.path.abspath(
        os.path.dirname(__file__)) + "/data/data/daily")
    bars = HistoricCSVDataHandler(event_queue,
                                  csv_dir,
                                  list(set(random.sample(symbol_list, 50))) +
                                  ["DUK", "AON", "C", "UAL", "AMZN", "COG"],
                                  start_date=start_date, fundamental=False,
                                  end_date=end_date
                                  )
else:
    bars = TDAData(event_queue, symbol_list, start_date, live=True)

filter = MultipleAllStrategy([
    ExtremaBounce(bars, event_queue, 7, 100, percentile=25),
    # RelativeExtrema(bars, event_queue,
    #     long_time=50,
    #     percentile=10, strat_contrarian=True),
    # LongTermCorrTrend(bars, event_queue, 150, corr=0.4, strat_contrarian=False),
    BoundedTA(bars, event_queue, 7, 20, floor=30, ceiling=70,
              ta_indicator=talib.RSI, ta_indicator_type=TAIndicatorType.TwoArgs),
    BoundedTA(bars, event_queue, 7, 20, floor=-100, ceiling=100,
              ta_indicator=talib.CCI, ta_indicator_type=TAIndicatorType.ThreeArgs),
])

# filter = MultipleAllStrategy([
#     ExtremaBounce(bars, event_queue, 10, 80, percentile=20),
#     LongTermCorrTrend(bars, event_queue, 200, corr=0.2,
#                       strat_contrarian=False),
#     OneSidedOrderOnly(bars, event_queue, OrderPosition.BUY)
# ])

signals = queue.Queue()
start = time.time()
while True:
    now = pd.Timestamp.now(tz=NY)
    if args.live and not (now.hour == 9 and now.minute == 35 and now.dayofweek <= 4):
        continue
    if bars.continue_backtest == True:
        logging.info(msg=f"{pd.Timestamp.now(tz=NY)}: update_bars")
        bars.update_bars()  # will take about 550s
    else:
        break

    if not event_queue.empty():
        event = event_queue.get(block=False)
        if event.type == 'MARKET':
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
        # logging.log(level=35, msg=f"{pd.Timestamp.now(tz=NY)}: sleeping")
        # time.sleep(8 * 3600)

signals = list(signals.queue)
print(f"Event loop finished in {time.time() - start}s.\n\
    Number of signals: {len(signals)}")
plot = PlotIndividual(bars, signals)
plot.plot()
