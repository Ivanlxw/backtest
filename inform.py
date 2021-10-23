import datetime
import logging
import os
import queue
import time
import random
from trading.utilities.enum import OrderPosition
from trading.portfolio.rebalance import RebalanceHalfYearly
import pandas as pd
from pathlib import Path
from sklearn.linear_model import Ridge, Lasso

from Inform.telegram import telegram_bot_sendtext
from trading.plots.plot import PlotIndividual
from trading.data.dataHandler import HistoricCSVDataHandler, NY, TDAData
from trading.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy, MultipleSendAllStrategy
from trading.strategy import ta, broad, fundamental, statistics
from trading.strategy.statmodels import features, targets, models
from backtest.utilities.utils import load_credentials, log_message, parse_args, generate_start_date, remove_bs
from Data.DataWriters.Prices import ABSOLUTE_BT_DATA_DIR
from backtest.strategy import profitable

args = parse_args()
load_credentials(args.credentials)
if args.name != "":
    logging.basicConfig(filename=Path(os.environ["WORKSPACE_ROOT"]) /
                        f"Data/logging/{args.name}.log", level=logging.INFO, force=True)
with open(ABSOLUTE_BT_DATA_DIR / "snp500.txt") as fin:
    SYM_LIST = list(map(remove_bs, fin.readlines()))

event_queue = queue.LifoQueue()
start_date = generate_start_date()
while pd.Timestamp(start_date).dayofweek > 4:
    start_date = generate_start_date()
print(start_date)
if not args.live:
    start_date = "2021-01-05"
    bars = HistoricCSVDataHandler(event_queue,
                   random.sample(SYM_LIST, 50),
                    # ["PYPL", "JPM", "ALLY", "BA", "DOW", "GM", "FB", "MA"],
                   start_date=None, # start_date,
                   frequency_type=args.frequency
                   )
else:
    bars = TDAData(event_queue, SYM_LIST, start_date, frequency_type=args.frequency, live=True)

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
        bars, event_queue, short_period=5, long_period=80, percentile=10),
    ta.VolAboveSMA(bars, event_queue, 10, OrderPosition.BUY),
    ta.TAMax(bars, event_queue, ta.rsi, 14, 7, OrderPosition.BUY),
    MultipleAnyStrategy(bars, event_queue, [
        fundamental.FundAtLeast(
            bars, event_queue, 'revenueGrowth', 0.03, order_position=OrderPosition.BUY),
        fundamental.FundAtLeast(
            bars, event_queue, 'netIncomeGrowth', 0.05, order_position=OrderPosition.BUY),
        fundamental.FundAtLeast(bars, event_queue, 'roe',
                                0, order_position=OrderPosition.BUY)
    ]),
])  # InformValueWithTA

feat = [
    features.RSI(14),
    features.RelativePercentile(50),
    features.DiffFromEMA(30),
    features.QuarterlyFundamental(bars, 'roe'),
    features.QuarterlyFundamental(bars, 'pbRatio'),
    # features.QuarterlyFundamental(bars, 'grossProfitGrowth')
]
target = targets.EMAClosePctChange(30)

strategy = models.SkLearnRegModelNormalized(
    bars, event_queue, Ridge, feat, target, RebalanceHalfYearly(bars, event_queue),
    order_val=0.08,
    n_history=60,
    params={
        "fit_intercept": False,
        "alpha": 0.5,
        # "max_depth": 4
    },
    live=args.live
)

strategy = MultipleAllStrategy(bars, event_queue, [
    strategy,
    MultipleAnyStrategy(bars, event_queue, [
        MultipleAllStrategy(bars, event_queue, [
            ta.TALessThan(bars, event_queue, ta.rsi,
                            14, 40, OrderPosition.BUY),
            ta.TAMax(bars, event_queue, ta.rsi, 14, 4, OrderPosition.BUY),
        ]),
        MultipleAllStrategy(bars, event_queue, [
            ta.TAMoreThan(bars, event_queue, ta.rsi,
                            14, 60, OrderPosition.SELL),
            ta.TAMin(bars, event_queue, ta.rsi, 14, 4, OrderPosition.SELL),
        ]),
    ])
])

strategy = MultipleSendAllStrategy(bars, event_queue, [
    # strategy, 
    strat_value,
    # profitable.momentum_with_TACross(bars, event_queue),
    # profitable.another_TA(bars, event_queue), # may not work well
    profitable.comprehensive_with_spy(bars, event_queue),
    profitable.bounce_ta(bars, event_queue),
    profitable.momentum_with_spy(bars, event_queue),    # buy only
    profitable.momentum_vol_with_spy(bars, event_queue),    # buy only
    profitable.value_extremaTA(bars, event_queue),
])

signals = queue.Queue()
start = time.time()
while True:
    now = pd.Timestamp.now(tz=NY)
    if now.dayofweek >= 4 and now.hour > 17:
        break
    time_since_midnight = now - now.normalize()
    if args.live and (time_since_midnight < datetime.timedelta(hours=9, minutes=45) or time_since_midnight > datetime.timedelta(hours=17, minutes=45)):
        time.sleep(60)
        continue
    if bars.continue_backtest == True:
        logging.info(msg=f"{pd.Timestamp.now(tz=NY)}: update_bars")
        bars.update_bars()
        # look at latest data just to see
        logging.info(f"{bars.get_latest_bars('DOW', N=20)}")
    else:
        break

    if not event_queue.empty():
        event = event_queue.get(block=False)
        if event.type == 'MARKET':
            signal_events = strategy.calculate_signals(event)
            logging.info(f"{pd.Timestamp.now(tz=NY)}: calculate signals")
            for signal_event in signal_events:
                if signal_event is not None:
                    signals.put(signal_event)
    if args.live:
        while not signals.empty():
            # TODO: send to phone via tele
            signal_event = signals.get(block=False)
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
