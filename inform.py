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
from backtest.utilities.utils import NY_TIMEZONE, generate_start_date_in_ms, get_sleep_time, load_credentials, log_message, parse_args, read_universe_list
from trading.event import SignalEvent
from trading.plots.plot import PlotIndividual
from trading.data.dataHandler import HistoricCSVDataHandler, DataFromDisk
from trading.strategy.fairprice.strategy import FairPriceStrategy
from trading.strategy.fairprice.feature import FeatureEMA, RelativeCCI, RelativeRSI, TradeImpulseBase, TradePressureEma, TrendAwareFeatureEMA
from trading.strategy.fairprice.margin import AsymmetricPercentageMargin
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
    # symbol_list = ['QQQM', 'SPY', 'MSFT', 'BRX', 'JPM', 'AMAM']
    if not args.live:
        bars = HistoricCSVDataHandler(symbol_list, creds,
                                      start_ms=start_ms,
                                      end_ms=end_ms,
                                      frequency_type=args.frequency
                                      )
    else:
        bars = DataFromDisk(read_universe_list(args.universe), creds,
                            start_ms, frequency_type=args.frequency)

    period = 15     # period to calculate algo
    ta_period = 14  # period of calculated values seen 
    feature = TrendAwareFeatureEMA(period + ta_period // 2) + RelativeRSI(ta_period, 10) + TradeImpulseBase(period // 2) + RelativeCCI(ta_period, 12)
    margin = AsymmetricPercentageMargin((0.03, 0.03) if args.frequency == "day" else (0.016, 0.01))
    strategy = FairPriceStrategy(bars, feature, margin, period + ta_period)

    signals = queue.Queue()
    start = time.time()
    is_fair_price_strategy = isinstance(strategy, FairPriceStrategy)
    historical_fair_prices = {sym: [] for sym in bars.symbol_data.keys()}
    while True:
        now = pd.Timestamp.now(tz=NY_TIMEZONE)
        time_since_midnight = now - now.normalize()
        if args.live and ((now.dayofweek == 4 and now.hour > 17) or now.dayofweek > 4):
            break
        elif args.live and (time_since_midnight < datetime.timedelta(hours=9, minutes=45) or time_since_midnight > datetime.timedelta(hours=17, minutes=45)):
            time.sleep(60)
            continue
        if bars.continue_backtest == True:
            log_message(f"{pd.Timestamp.now(tz=NY_TIMEZONE)}: update_bars")
            bars.update_bars(event_queue)
        else:
            break

        if not event_queue.empty():
            event = event_queue.get(block=False)
            if event.type == 'MARKET':
                signal_events: List[SignalEvent] = strategy.calculate_signals(
                    event, historical_fair_prices if is_fair_price_strategy else None)
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
