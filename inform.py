from backtest.strategy.fundamental import HighRevGain, LowDCF
import json
import logging
import os
import queue
import time
import random
from trading.utilities.enum import OrderPosition
from trading.strategy.naive import Strategy
from trading.strategy.basic import OneSidedOrderOnly
from trading.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy

import talib
import pandas as pd

from backtest.utilities.utils import parse_args
from backtest.utilities.utils import generate_start_date
from trading.data.dataHandler import FMPData, HistoricCSVDataHandler, NY, TDAData
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
if not args.live:
    end_date = "2020-01-30"
    bars = HistoricCSVDataHandler(event_queue,
                                  list(set(random.sample(symbol_list, 75) +
                                           ["DUK", "AON", "C", "UAL", "AMZN", "COG"])),
                                  start_date=start_date,
                                  end_date=end_date
                                  )

    # bars = FMPData(event_queue, random.sample(symbol_list, 75), start_date,
    #                frequency_type="daily")
else:
    bars = TDAData(event_queue, symbol_list, start_date, live=True)

filter = MultipleAllStrategy([
    RelativeExtrema(bars, event_queue,
                    long_time=100,
                    percentile=5, strat_contrarian=True),
    BoundedTA(bars, event_queue, 7, 20, floor=30, ceiling=70,
              ta_indicator=talib.RSI, ta_indicator_type=TAIndicatorType.TwoArgs),
    BoundedTA(bars, event_queue, 7, 20, floor=-100, ceiling=100,
              ta_indicator=talib.CCI, ta_indicator_type=TAIndicatorType.ThreeArgs),
    OneSidedOrderOnly(bars, event_queue, OrderPosition.BUY)
])

# filter = MultipleAllStrategy([
#     ExtremaBounce(bars, event_queue, 10, 100, percentile=15),
#     MeanReversionTA(bars, event_queue, 20, talib.SMA, exit=False),
#     ExtremaTA(bars, event_queue, talib.RSI, 14, TAIndicatorType.TwoArgs,
#               7, strat_contrarian=False, consecutive=1),
#     OneSidedOrderOnly(bars, event_queue, OrderPosition.BUY)
# ])

filter = MultipleAllStrategy([
    OneSidedOrderOnly(bars, event_queue, OrderPosition.BUY),
    # LowDCF(bars, event_queue, buy_ratio=1.25, sell_ratio=3.2),
    HighRevGain(bars, event_queue, perc=5),
    RelativeExtrema(bars, event_queue,
                    long_time=100,
                    percentile=5, strat_contrarian=True),
    BoundedTA(bars, event_queue, 7, 20, floor=30, ceiling=70,
              ta_indicator=talib.RSI, ta_indicator_type=TAIndicatorType.TwoArgs)
    # ExtremaBounce(bars, event_queue, 7, 50, percentile=25),
])

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
