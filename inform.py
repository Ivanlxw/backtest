import datetime
import logging
import os
import queue
import random
import time
from typing import List
import pandas as pd
from pathlib import Path

from Inform.telegram import telegram_bot_sendtext
from backtest.utilities.utils import generate_start_date_after_2015, load_credentials, log_message, parse_args
from backtest.strategy import profitable
from trading.event import SignalEvent
from trading.plots.plot import PlotIndividual
from trading.data.dataHandler import HistoricCSVDataHandler, NY, DataFromDisk
from trading.strategy.basic import OneSidedOrderOnly
from trading.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy, MultipleSendAllStrategy
from trading.strategy import ta, broad, fundamental, statistics
from trading.utilities.enum import OrderPosition
from trading.utilities.utils import get_etf_list, get_trading_universe

ETF_LIST = get_etf_list(Path(os.path.dirname(os.path.realpath(__file__))))
if __name__ == "__main__":
    args = parse_args()
    creds = load_credentials(args.credentials)
    if args.name != "":
        logging.basicConfig(filename=Path(os.environ["WORKSPACE_ROOT"]) /
                            f"Data/logging/{args.name}.log", level=logging.INFO, force=True)

    event_queue = queue.LifoQueue()
    start_date = generate_start_date_after_2015()
    while pd.Timestamp(start_date).dayofweek > 4:
        start_date = generate_start_date_after_2015()
    print(start_date)
    universe_list = get_trading_universe(args.universe)
    symbol_list = random.sample(universe_list, min(len(universe_list), 50))
    if not args.live:
        bars = HistoricCSVDataHandler(event_queue, symbol_list,
                                      creds,
                                      start_date=start_date,
                                      frequency_type=args.frequency
                                      )
    else:
        bars = DataFromDisk(event_queue, get_trading_universe(args.universe), creds,
                            start_date, frequency_type=args.frequency, live=True)

    strat_pre_momentum = MultipleAllStrategy(bars, event_queue, [  # any of buy and sell
        statistics.ExtremaBounce(
            bars, event_queue, short_period=8, long_period=80, percentile=40),
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
            MultipleAnyStrategy(bars, event_queue, [   # sell
                # RelativeExtrema(bars, event_queue, 20, strat_contrarian=False),
                ta.TAMoreThan(bars, event_queue, ta.rsi,
                              14, 50, OrderPosition.SELL),
                ta.TAMoreThan(bars, event_queue, ta.cci,
                              14, 20, OrderPosition.SELL),
                ta.TAMin(bars, event_queue, ta.rsi, 14, 5, OrderPosition.SELL),
                broad.below_functor(bars, event_queue, 'SPY',
                                    20, OrderPosition.SELL),
            ], min_matches=2)
        ])
    ])  # StratPreMomentum

    if args.frequency == "daily":
        strategy =  profitable.strict_comprehensive_momentum(bars, event_queue, -0.05)
    else:   # intraday
        # strategy = MultipleSendAllStrategy(bars, event_queue, [
        #     profitable.high_beta_momentum(
        #         bars, event_queue, trending_score=-0.1),
        #     # profitable.momentum_with_spy(bars, event_queue),    # buy only
        #     # profitable.momentum_vol_with_spy(bars, event_queue),    # buy only
        #     profitable.value_extremaTA(bars, event_queue),
        #     profitable.trending_ma(bars, event_queue, trending_score=0),
        #     # ta.MABounce(bars, event_queue, ta.ema, 25)
        # ])
        strategy =  profitable.strict_comprehensive_momentum(bars, event_queue, -0.05)

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
        else:
            break

        if not event_queue.empty():
            event = event_queue.get(block=False)
            if event.type == 'MARKET':
                signal_events: List[SignalEvent] = strategy.calculate_signals(
                    event)
                for signal_event in signal_events:
                    if signal_event is not None:
                        signals.put(signal_event)
        if args.live:
            while not signals.empty():
                # TODO: send to phone via tele
                signal_event: SignalEvent = signals.get(block=False)
                if signal_event.symbol in ETF_LIST:
                    res = telegram_bot_sendtext(f"[{args.frequency}]\n{args.name:}\n"+signal_event.details(),
                                                creds["TELEGRAM_APIKEY_ETF"], creds["TELEGRAM_CHATID"])
                else:
                    res = telegram_bot_sendtext(f"[{args.frequency}]\n{args.name:}\n"+signal_event.details(),
                                                creds["TELEGRAM_APIKEY"], creds["TELEGRAM_CHATID"])
            log_message("sleeping")
            time.sleep(args.sleep_time)
            log_message("sleep over")

    signals = list(signals.queue)
    print(f"Event loop finished in {time.time() - start}s.\n\
        Number of signals: {len(signals)}")
    plot = PlotIndividual(bars, signals)
    plot.plot()
