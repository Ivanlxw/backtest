import datetime
import logging
import os
import queue
import time
from typing import List
import pandas as pd
from pathlib import Path

from Inform.telegram import telegram_bot_sendtext
from trading.event import SignalEvent
from trading.plots.plot import PlotIndividual
from trading.data.dataHandler import HistoricCSVDataHandler, NY, DataFromDisk
from trading.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy, MultipleSendAllStrategy
from trading.strategy import ta, broad, fundamental, statistics
from trading.strategy.complex.complex_high_beta import ComplexHighBeta
from trading.utilities.enum import OrderPosition
from backtest.utilities.utils import load_credentials, log_message, parse_args, generate_start_date
from Data.DataWriters.Prices import DOW_LIST, SNP_LIST, NASDAQ_LIST, ETF_LIST
from backtest.strategy import profitable


SYM_LIST = DOW_LIST + SNP_LIST + NASDAQ_LIST 
args = parse_args()
load_credentials(args.credentials)
if args.name != "":
    logging.basicConfig(filename=Path(os.environ["WORKSPACE_ROOT"]) /
                        f"Data/logging/{args.name}.log", level=logging.INFO, force=True)

event_queue = queue.LifoQueue()
start_date = generate_start_date()
while pd.Timestamp(start_date).dayofweek > 4:
    start_date = generate_start_date()
print(start_date)
if not args.live:
    bars = HistoricCSVDataHandler(event_queue,
                   DOW_LIST + ETF_LIST,
                   start_date=start_date,
                   frequency_type=args.frequency
                   )
else:
    bars = DataFromDisk(event_queue, SYM_LIST + ETF_LIST, start_date, frequency_type=args.frequency, live=True)

strat_pre_momentum = MultipleAllStrategy(bars, event_queue, [  # any of buy and sell
    statistics.ExtremaBounce(
        bars, event_queue, short_period=6, long_period=80, percentile=45),
    MultipleAnyStrategy(bars, event_queue, [
        MultipleAllStrategy(bars, event_queue, [   # buy
            MultipleAnyStrategy(bars, event_queue, [
                fundamental.FundAtLeast(bars, event_queue,
                                        'revenueGrowth', 0.1, order_position=OrderPosition.BUY),
                fundamental.FundAtLeast(bars, event_queue, 'roe',
                                        0, order_position=OrderPosition.BUY),
            ]),
            ta.TALessThan(bars, event_queue, ta.cci,
                          20, 0, OrderPosition.BUY),
        ]),
        MultipleAllStrategy(bars, event_queue, [   # sell
            # RelativeExtrema(bars, event_queue, 20, strat_contrarian=False),
            ta.TAMoreThan(bars, event_queue, ta.rsi,
                          14, 50, OrderPosition.SELL),
            ta.TAMoreThan(bars, event_queue, ta.cci,
                          14, 20, OrderPosition.SELL),
            ta.TAMin(bars, event_queue, ta.rsi, 14, 5, OrderPosition.SELL),
            broad.below_sma(bars, event_queue, 'SPY',
                            20, OrderPosition.SELL),
        ])
    ])
])  # StratPreMomentum


strat_value = MultipleAllStrategy(bars, event_queue, [
    statistics.ExtremaBounce(
        bars, event_queue, short_period=7, long_period=100, percentile=10),
    statistics.EitherSide(bars, event_queue, 100, 20),
    MultipleAnyStrategy(bars, event_queue, [
        MultipleAllStrategy(bars, event_queue, [
            fundamental.FundAtLeast(
                bars, event_queue, 'threeYRevenueGrowthPerShare', 0.5, order_position=OrderPosition.BUY),
            fundamental.FundAtLeast(
                bars, event_queue, 'operatingIncomeGrowth', 0.1, order_position=OrderPosition.BUY),
            fundamental.FundAtLeast(bars, event_queue, 'roe',
                                    0, order_position=OrderPosition.BUY),
            ta.VolAboveSMA(bars, event_queue, 10, OrderPosition.BUY),
            ta.TAMax(bars, event_queue, ta.rsi, 14, 7, OrderPosition.BUY),
        ]),  # buy
        MultipleAllStrategy(bars, event_queue, [
            ta.TAMin(bars, event_queue, ta.rsi, 14, 7, OrderPosition.SELL),   
            ta.TAMin(bars, event_queue, ta.cci, 20, 7, OrderPosition.SELL),   
        ])  # sell
    ]),
], "StratValue")

strategy = ComplexHighBeta(bars, event_queue, ["SPY", "XLK", "QQQ"],
    index_strategy=MultipleAllStrategy(bars, event_queue, [
        statistics.ExtremaBounce(
            bars, event_queue, short_period=5, long_period=80, percentile=20),
        MultipleAnyStrategy(bars, event_queue, [
            ta.TAMax(bars, event_queue, ta.rsi, 14, 7, OrderPosition.BUY),
            ta.TAMin(bars, event_queue, ta.rsi, 14, 7, OrderPosition.SELL),
        ])
    ]), corr_days=60, corr_min=0.85, description="HighBetaValue")

if args.frequency == "daily": 
    strategy = MultipleSendAllStrategy(bars, event_queue, [
        profitable.bounce_ta(bars, event_queue),
        profitable.comprehensive_longshort(bars, event_queue),
        strategy,
        # profitable.dcf_value_growth_v2(bars, event_queue), 
        # profitable.momentum_with_TACross(bars, event_queue),
        # profitable.momentum_with_spy(bars, event_queue),
        # profitable.comprehensive_with_spy(bars, event_queue),
        # profitable.momentum_vol_with_spy(bars, event_queue),    # buy only
        # profitable.another_TA(bars, event_queue), # may not work well
    ])
else:   # intraday
    strategy = MultipleSendAllStrategy(bars, event_queue, [
        strategy,
        profitable.momentum_with_spy(bars, event_queue),    # buy only
        profitable.momentum_vol_with_spy(bars, event_queue),    # buy only
        # profitable.momentum_with_TACross(bars, event_queue),
        # profitable.another_TA(bars, event_queue), # may not work well
        profitable.bounce_ta(bars, event_queue),
        profitable.value_extremaTA(bars, event_queue),
    ])

signals = queue.Queue()
start = time.time()
while True:
    now = pd.Timestamp.now(tz=NY)
    time_since_midnight = now - now.normalize()
    if args.live and (time_since_midnight < datetime.timedelta(hours=9, minutes=45) or time_since_midnight > datetime.timedelta(hours=17, minutes=45)):
        if now.dayofweek > 4:
            break
        time.sleep(60)
        continue
    if bars.continue_backtest == True:
        log_message(f"{pd.Timestamp.now(tz=NY)}: update_bars")
        bars.update_bars()
        # look at latest data just to see
        log_message(f"Latest bars: {bars.get_latest_bars('DOW', N=20)}")
    else:
        break

    if not event_queue.empty():
        event = event_queue.get(block=False)
        if event.type == 'MARKET':
            log_message(f"{pd.Timestamp.now(tz=NY)}: calculate signals")
            signal_events: List[SignalEvent] = strategy.calculate_signals(event)
            for signal_event in signal_events:
                if signal_event is not None:
                    signals.put(signal_event)
    if args.live:
        while not signals.empty():
            # TODO: send to phone via tele
            signal_event: SignalEvent = signals.get(block=False)
            if signal_event.symbol in ETF_LIST:
                res = telegram_bot_sendtext(f"[{args.frequency}]\n{args.name:}\n"+signal_event.details(),
                            os.environ["TELEGRAM_APIKEY_ETF"], os.environ["TELEGRAM_CHATID"])
            else:
                res = telegram_bot_sendtext(f"[{args.frequency}]\n{args.name:}\n"+signal_event.details(),
                                            os.environ["TELEGRAM_APIKEY"], os.environ["TELEGRAM_CHATID"])
        log_message("sleeping")
        time.sleep(args.sleep_time)
        log_message("sleep over")


signals = list(signals.queue)
print(f"Event loop finished in {time.time() - start}s.\n\
    Number of signals: {len(signals)}")
plot = PlotIndividual(bars, signals)
plot.plot()
