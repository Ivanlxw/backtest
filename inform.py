import json
import logging
import os
import queue
import time
import json
import random
from trading.strategy.basic import OneSidedOrderOnly
from trading.utilities.enum import OrderPosition
from trading.portfolio.rebalance import RebalanceHalfYearly
import pandas as pd
from pathlib import Path
from sklearn.linear_model import Ridge, Lasso
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.svm import SVR

from Inform.telegram import telegram_bot_sendtext
from trading.plots.plot import PlotIndividual
from trading.data.dataHandler import HistoricCSVDataHandler, NY, TDAData
from trading.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy, MultipleSendAllStrategy
from trading.strategy import ta, broad, fundamental, statistics
from trading.strategy.statmodels import features, targets, models
from backtest.utilities.utils import MODELINFO_DIR, load_credentials, log_message, parse_args, generate_start_date

args = parse_args()
load_credentials(args.credentials)
if args.name != "":
    logging.basicConfig(filename=Path(os.environ["WORKSPACE_ROOT"]) /
                        f"Data/logging/{args.name}.log", level=logging.INFO, force=True)
with open("./Data/snp500.txt", 'r') as fin:
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
                                  random.sample(symbol_list, 30),
                                  start_date=start_date,
                                  end_date=end_date
                                  )
else:
    bars = TDAData(event_queue, symbol_list, start_date, live=True)

strat_momentum = MultipleAllStrategy(bars, event_queue, [
    statistics.ExtremaBounce(
        bars, event_queue, short_period=6, long_period=80, percentile=50),
    broad.above_sma(bars, event_queue, 'SPY', 25, OrderPosition.BUY),
    ta.TAMax(bars, event_queue, ta.rsi, 14, 7, OrderPosition.BUY),
    MultipleAnyStrategy(bars, event_queue, [
        fundamental.FundAtLeast(bars, event_queue,
                                'revenueGrowth', 0.1, order_position=OrderPosition.BUY),
        fundamental.FundAtLeast(bars, event_queue, 'roe',
                                0, order_position=OrderPosition.BUY)
    ])
])  #StratMomentum

strat_value = MultipleAllStrategy(bars, event_queue, [
    statistics.ExtremaBounce(
        bars, event_queue, short_period=5, long_period=80, percentile=10),
    # RelativeExtrema(bars, event_queue, long_time=50, percentile=10, strat_contrarian=True),
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

# strategy = MultipleAnyStrategy(bars, event_queue, [  # any of buy and sell
#     MultipleAllStrategy(bars, event_queue, [   # buy
#         ta.TALessThan(bars, event_queue, ta.rsi,
#                         14, 45, OrderPosition.BUY),
#         broad.above_sma(bars, event_queue, 'SPY',
#                         25, OrderPosition.BUY),
#         ta.TALessThan(bars, event_queue, ta.cci,
#                       20, -70, OrderPosition.BUY),
#         ta.TAMax(bars, event_queue, ta.rsi, 14, 5, OrderPosition.BUY)
#     ]),
#     MultipleAllStrategy(bars, event_queue, [   # sell
#         # RelativeExtrema(bars, event_queue, 20, strat_contrarian=False),
#         ta.TAMoreThan(bars, event_queue, ta.rsi,
#                       14, 50, OrderPosition.SELL),
#         ta.TAMoreThan(bars, event_queue, ta.cci,
#                       14, 70, OrderPosition.SELL),
#         ta.TAMin(bars, event_queue, ta.rsi, 14, 5, OrderPosition.SELL),
#         broad.below_sma(bars, event_queue, 'SPY',
#                         20, OrderPosition.SELL),
#     ])
# ])  # TAWithSpy

feat = [
    features.RSI(14),
    features.RelativePercentile(50),
    features.QuarterlyFundamental(bars, 'roe'),
    features.QuarterlyFundamental(bars, 'pbRatio'),
    features.QuarterlyFundamental(bars, 'grossProfitGrowth')
]
target = targets.EMAClosePctChange(30)

strategy = models.SkLearnRegModelNormalized(
    bars, event_queue, SVR, feat, target, RebalanceHalfYearly,
    order_val=0.08,
    n_history=60,
    params={
        # "fit_intercept": False,
        "C": 2,
        # "alpha": 0.5,
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
            # ta.TAMax(bars, event_queue, ta.rsi, 14, 5, OrderPosition.BUY),
        ]),
        MultipleAllStrategy(bars, event_queue, [
            ta.TAMoreThan(bars, event_queue, ta.rsi,
                          14, 60, OrderPosition.SELL),
            # ta.TAMin(bars, event_queue, ta.rsi, 14, 6, OrderPosition.SELL),
        ]),
    ])
])

# strategy = MultipleSendAllStrategy(bars, event_queue, [
#     strat_value, strat_momentum
# ])


if args.name != "":
    with open(MODELINFO_DIR / f'{args.name}.json', 'w') as fout:
        fout.write(json.dumps(strategy.describe()))

signals = queue.Queue()
start = time.time()
while True:
    now = pd.Timestamp.now(tz=NY)
    if args.live and not (now.hour == 9 and now.minute > 45):
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
            signal_events = strategy.calculate_signals(event)
            logging.info(f"{pd.Timestamp.now(tz=NY)}: calculate signals")
            for signal_event in signal_events:
                if signal_event is not None:
                    signals.put(signal_event)
    if args.live:
        while not signals.empty():
            # TODO: send to phone via tele
            signal_event = signals.get(block=False)
            logging.info(signal_event.details())
            res = telegram_bot_sendtext(f"{args.name:}\n"+signal_event.details(),
                                        os.environ["TELEGRAM_APIKEY"], os.environ["TELEGRAM_CHATID"])
        if now.dayofweek >= 4:
            break
        log_message("sleeping")
        time.sleep(16 * 3600)
        log_message("sleep over")


signals = list(signals.queue)
print(f"Event loop finished in {time.time() - start}s.\n\
    Number of signals: {len(signals)}")
plot = PlotIndividual(bars, signals)
plot.plot()
