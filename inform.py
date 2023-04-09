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
from backtest.utilities.utils import generate_start_date_in_ms, get_sleep_time, load_credentials, log_message, parse_args, read_universe_list
# from backtest.strategy import profitable
from trading.event import SignalEvent
from trading.plots.plot import PlotIndividual
from trading.data.dataHandler import HistoricCSVDataHandler, NY, DataFromDisk
from trading.strategy.fairprice.strategy import FairPriceStrategy
from trading.strategy.fairprice.feature import FeatureSMA, RSIFromBaseLine, RelativeRSI
from trading.strategy.fairprice.margin import PercentageMargin
from trading.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy
from trading.strategy import ta, broad, statistics
from trading.utilities.enum import OrderPosition

if __name__ == "__main__":
    args = parse_args()
    creds = load_credentials(args.credentials)
    data_dir = Path(os.environ["DATA_DIR"])
    if args.name != "":
        logging.basicConfig(filename=data_dir / f"logging/{args.name}.log", level=logging.INFO, force=True)

    event_queue = queue.LifoQueue()
    if args.start_ms is not None:
        start_ms = args.start_ms
    else:
        start_ms = generate_start_date_in_ms(2021, 2022)
    # end anytime between 50 - 400 days later
    end_ms = int(start_ms + random.randint(250, 700) * 8.64e7 * (1.0 if args.frequency == "day" else 0.2))  # end anytime between 200 - 800 days later
    universe_list = read_universe_list(args.universe)
    etf_list = read_universe_list([data_dir / "universe/etf.txt"])
    symbol_list = random.sample(universe_list, min(len(universe_list), 25))
    if not args.live:
        bars = HistoricCSVDataHandler(event_queue, symbol_list,
                                      creds,
                                      start_ms=start_ms,
                                      end_ms=end_ms,
                                      frequency_type=args.frequency
                                      )
    else:
        bars = DataFromDisk(event_queue, read_universe_list(args.universe), creds,
                            start_ms, frequency_type=args.frequency, live=True)

    '''
    strategy = MultipleAllStrategy(bars, event_queue, [  # any of buy and sell
        statistics.ExtremaBounce(
            bars, event_queue, short_period=8, long_period=65, percentile=40),
        MultipleAnyStrategy(bars, event_queue, [
                            MultipleAllStrategy(bars, event_queue, [   # buy
                                # MultipleAnyStrategy(bars, event_queue, [
                                #     fundamental.FundAtLeast(bars, event_queue,
                                #                             'revenueGrowth', 0.1, order_position=OrderPosition.BUY),
                                #     fundamental.FundAtLeast(bars, event_queue, 'roe',
                                #                             0, order_position=OrderPosition.BUY),
                                # ]),
                                ta.TALessThan(bars, event_queue, ta.cci,
                                              20, 0, OrderPosition.BUY),
                                broad.above_functor(bars, event_queue, 'SPY',
                                                    20, args.frequency, OrderPosition.BUY),
                            ]),
                            MultipleAnyStrategy(bars, event_queue, [   # sell
                                # RelativeExtrema(bars, event_queue, 20, strat_contrarian=False),
                                ta.TAMoreThan(bars, event_queue, ta.rsi,
                                              14, 50, OrderPosition.SELL),
                                ta.TAMoreThan(bars, event_queue, ta.cci,
                                              14, 20, OrderPosition.SELL),
                                ta.TAMin(bars, event_queue, ta.rsi, 14, 5, OrderPosition.SELL),
                                broad.below_functor(bars, event_queue, 'SPY',
                                                    20, args.frequency, OrderPosition.SELL),
                            ], min_matches=2)
                            ])
    ])  # StratPreMomentum
    strategy = MultipleAllStrategy(bars, event_queue, [
        strategy,
        statistics.RelativeExtrema(bars, event_queue, 35, strat_contrarian=True, percentile=10)
    ])
    # strategy = MultipleAnyStrategy(bars, event_queue, [
    #     profitable.strict_comprehensive_longshort(
    #         bars, event_queue, ma_value=22, trending_score=-0.05),
    #     strat_pre_momentum])
    '''

    # strategy = MultipleAllStrategy(bars, event_queue, [  # any of buy and sell
    #     statistics.ExtremaBounce(
    #         bars, event_queue, short_period=8, long_period=65, percentile=35),
    #     MultipleAnyStrategy(bars, event_queue, [
    #         broad.above_functor(bars, event_queue, 'SPY', 20, bars.frequency_type, OrderPosition.BUY),
    #         broad.below_functor(bars, event_queue, 'SPY', 20, bars.frequency_type, OrderPosition.SELL),
    #         # ta.MABounce(bars, event_queue, ta.sma, 20),
    #         ta.SimpleTACross(bars, event_queue, 20, ta.ema)
    #     ], min_matches=2)
    # ])
    period = 15
    feature = FeatureSMA(period) + RSIFromBaseLine(period, 47) + RelativeRSI(period, 7)  #need at least period + 7
    margin = PercentageMargin(0.012)
    strategy = FairPriceStrategy(bars, event_queue, feature, margin, period + 8)

    signals = queue.Queue()
    start = time.time()
    is_fair_price_strategy = isinstance(strategy, FairPriceStrategy)
    historical_fair_prices = {sym: [] for sym in bars.symbol_data.keys()}
    while True:
        now = pd.Timestamp.now(tz=NY)
        time_since_midnight = now - now.normalize()
        if args.live and ((now.dayofweek == 4 and now.hour > 17) or now.dayofweek > 4):
            break
        elif args.live and (time_since_midnight < datetime.timedelta(hours=9, minutes=45) or time_since_midnight > datetime.timedelta(hours=17, minutes=45)):
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
                    event, historical_fair_prices) if is_fair_price_strategy else strategy.calculate_signals(
                    event)
                for signal_event in signal_events:
                    if signal_event is not None:
                        signals.put(signal_event)
        if args.live:
            while not signals.empty():
                signal_event: SignalEvent = signals.get(block=False)
                log_message(signal_event.details())
                res = telegram_bot_sendtext(f"{args.frequency}\n{signal_event.details()}",
                            creds["TELEGRAM_APIKEY"], creds["TELEGRAM_CHATID"])
                # if signal_event.symbol in etf_list:
                #     telegram_bot_sendtext(f"{args.frequency}\n{signal_event.details()}",
                #             creds["TELEGRAM_APIKEY_ETF"], creds["TELEGRAM_CHATID"])
                # else:
                #     res = telegram_bot_sendtext(f"{args.frequency}\n{signal_event.details()}", 
                #                                 bot_apikey=creds["TELEGRAM_APIKEY"], bot_chatid=creds["TELEGRAM_CHATID"])
            log_message("sleeping")
            time.sleep(get_sleep_time(args.frequency))
            log_message("sleep over")

    signals = list(signals.queue)
    print(f"Event loop finished in {time.time() - start}s.\n"
          f"Number of BUY signals: {len([sig for sig in signals if sig.order_position == OrderPosition.BUY])}"
          f"\nNumber of SELL signals: {len([sig for sig in signals if sig.order_position == OrderPosition.SELL])}")
    plot = PlotIndividual(strategy, signals, is_fair_price_strategy, historical_fair_prices)
    plot.plot()
