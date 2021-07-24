import json
import logging
import os
import queue
import time
import random
import talib
import pandas as pd
from pathlib import Path

from backtest.utilities.utils import load_credentials, parse_args
from backtest.utilities.utils import generate_start_date
from Inform.telegram import telegram_bot_sendtext
from trading.strategy.statistics import ExtremaBounce, LongTermCorrTrend, RelativeExtrema
from trading.strategy.ta import BoundedTA, ExtremaTA, MeanReversionTA, TAIndicatorType
from trading.plots.plot import PlotIndividual
from trading.data.dataHandler import HistoricCSVDataHandler, NY, TDAData
from trading.strategy.fundamental import HighRevGain, LowDCF
from trading.strategy.multiple import MultipleAllStrategy


ABSOLUTE_FILEDIR = Path(os.path.dirname(os.path.abspath(__file__)))
args = parse_args()
load_credentials(args.credentials)
if args.name != "":
    logging.basicConfig(filename=ABSOLUTE_FILEDIR /
                        f"Data/logging/{args.name}.log", level=logging.INFO)
with open("./Data/downloaded_universe.txt", 'r') as fin:
    stock_list = fin.readlines()
stock_list = list(map(lambda x: x.replace('\n', ''), stock_list))
symbol_list = stock_list

event_queue = queue.LifoQueue()
start_date = generate_start_date()
while pd.Timestamp(start_date).dayofweek > 4:
    start_date = generate_start_date()
print(start_date)
if not args.live:
    end_date = "2020-01-30"
    bars = HistoricCSVDataHandler(event_queue,
                                  random.sample(symbol_list, 250),
                                  start_date=start_date,
                                  end_date=end_date
                                  )
    # bars = FMPData(event_queue, random.sample(symbol_list, 75), start_date,
    #                frequency_type="daily")
else:
    bars = TDAData(event_queue, symbol_list, start_date, live=True)

# filter = MultipleAllStrategy([
#     HighRevGain(bars, event_queue, perc=3),
#     ExtremaBounce(bars, event_queue, 10, 100, percentile=15),
#     MeanReversionTA(bars, event_queue, 20, talib.SMA, exit=False),
#     ExtremaTA(bars, event_queue, talib.RSI, 14, TAIndicatorType.TwoArgs,
#               7, strat_contrarian=False, consecutive=1),
# ])

# filter = MultipleAllStrategy([
#     # OneSidedOrderOnly(bars, event_queue, OrderPosition.BUY),
#     # LowDCF(bars, event_queue, buy_ratio=2, sell_ratio=6),
#     RelativeExtrema(bars, event_queue,
#                     long_time=100,
#                     percentile=5, strat_contrarian=True),
#     BoundedTA(bars, event_queue, 7, 14, floor=30, ceiling=70,
#               ta_indicator=talib.RSI, ta_indicator_type=TAIndicatorType.TwoArgs),
#     BoundedTA(bars, event_queue, 7, 20, floor=-100, ceiling=200,
#               ta_indicator=talib.CCI, ta_indicator_type=TAIndicatorType.ThreeArgs),
#     HighRevGain(bars, event_queue, perc=3),
# ])

filter = MultipleAllStrategy([
    ExtremaBounce(bars, event_queue, 8, 100, percentile=15),
    HighRevGain(bars, event_queue, perc=4)
])

signals = queue.Queue()
start = time.time()
while True:
    now = pd.Timestamp.now(tz=NY)
    if args.live and not (now.hour == 10 and now.dayofweek <= 4):
        continue
    if bars.continue_backtest == True:
        logging.info(msg=f"{pd.Timestamp.now(tz=NY)}: update_bars")
        bars.update_bars()  # will take about 550s
        # look at latest data just to see
        logging.info(f"{bars.get_latest_bars(bars.symbol_list[-1], N=20)}")
    else:
        break

    if not event_queue.empty():
        event = event_queue.get(block=False)
        if event.type == 'MARKET':
            signal_events = filter.calculate_signals(event)
            logging.info(f"{pd.Timestamp.now(tz=NY)}: calculate signals")
            for signal_event in signal_events:
                if signal_event is not None:
                    signals.put(signal_event)
    if args.live:
        while not signals.empty():
            # TODO: send to phone via tele
            signal_event = signals.get(block=False)
            logging.info(signal_event.details())
            res = telegram_bot_sendtext(signal_event.details(),
                                        os.environ["TELEGRAM_APIKEY"], os.environ["TELEGRAM_CHATID"])
        logging.info(f"{pd.Timestamp.now(tz=NY)}: sleeping")
        time.sleep(16 * 3600)
        logging.info("sleep over")


signals = list(signals.queue)
print(f"Event loop finished in {time.time() - start}s.\n\
    Number of signals: {len(signals)}")
plot = PlotIndividual(bars, signals)
plot.plot()
