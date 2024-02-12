from collections import deque
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
from backtest.utilities.utils import (
    generate_start_date_in_ms,
    get_sleep_time,
    load_credentials,
    log_message,
    parse_args,
    read_universe_list,
)
from trading.event import SignalEvent
from trading.plots.plot import PlotIndividual
from trading.data.dataHandler import HistoricCSVDataHandler, DataFromDisk
from trading.portfolio.portfolio import NaivePortfolio
from trading.strategy.base import Strategy
from trading.strategy.fairprice.strategy import FairPriceStrategy
from trading.strategy.fairprice.feature import FeatureEMA, RelativeCCI, RelativeRSI, TradeImpulseBase
from trading.strategy.fairprice.margin import AsymmetricPercentageMargin
from trading.strategy.statmodels.models import EquityPrediction
from trading.utilities.enum import OrderPosition
from trading.utilities.utils import DATA_DIR, NY_TIMEZONE


MODEL_DIR = Path(os.environ["DATA_DIR"]) / "models"


def run(args, creds, strategy):
    event_queue = deque()
    if args.start_ms is not None:
        start_ms = args.start_ms
    else:
        start_ms = generate_start_date_in_ms(2021, 2022)
    end_ms = int(start_ms + random.randint(250, 700) * 8.64e7 * (1.0 if args.frequency == "day" else 0.2))
    universe_list = read_universe_list([DATA_DIR / "universe/etf.txt", DATA_DIR / "universe/nyse_active.txt"])
    symbol_list = random.sample(universe_list, min(len(universe_list), 9))
    if not args.live:
        bars = HistoricCSVDataHandler(
            symbol_list, creds, start_ms=start_ms, end_ms=end_ms, frequency_type=args.frequency
        )
    else:
        bars = DataFromDisk(read_universe_list(args.universe), creds, start_ms, frequency_type=args.frequency)
    portfolio = NaivePortfolio(1, "inform")
    portfolio.Initialize(
        symbol_list,
        bars.start_ms,
        bars.option_metadata_info
    )
    portfolio.set_keep_historical_data_period(int(1e6))

    signals = queue.Queue()
    start = time.time()
    while True:
        now = pd.Timestamp.now(tz=NY_TIMEZONE)
        time_since_midnight = now - now.normalize()
        if args.live and ((now.dayofweek == 4 and now.hour > 17) or now.dayofweek > 4):
            break
        elif args.live and (
            time_since_midnight < datetime.timedelta(hours=9, minutes=45)
            or time_since_midnight > datetime.timedelta(hours=17, minutes=45)
        ):
            time.sleep(60)
            continue
        if bars.continue_backtest == True:
            log_message(f"{pd.Timestamp.now(tz=NY_TIMEZONE)}: update_bars")
            bars.update_bars(event_queue)
        else:
            break

        if len(event_queue) != 0:
            event = event_queue.pop()
            if event.type == "MARKET":
                market_data = event.data
                if portfolio.update_timeindex(market_data, event_queue):
                    inst = portfolio.current_holdings[event.symbol]
                    signal_events: List[SignalEvent] = strategy.calculate_signals(event, inst)
                    for signal_event in signal_events:
                        if signal_event is not None:
                            signals.put(signal_event)
        if args.live:
            while not signals.empty():
                signal_event: SignalEvent = signals.get(block=False)
                log_message(signal_event.details())
                res = telegram_bot_sendtext(
                    f"{args.frequency}\n{signal_event.details()}", creds["TELEGRAM_APIKEY"], creds["TELEGRAM_CHATID"]
                )
            log_message(f"[{datetime.datetime.now()}] sleeping")
            time.sleep(get_sleep_time(args.frequency))
            log_message(f"[{datetime.datetime.now()}] sleep over")

    signals = list(signals.queue)
    print(
        f"Event loop finished in {time.time() - start}s.\n"
        f"Number of BUY signals: {len([sig for sig in signals if sig.order_position == OrderPosition.BUY])}"
        f"\nNumber of SELL signals: {len([sig for sig in signals if sig.order_position == OrderPosition.SELL])}"
    )
    historical_mkt_prices = {sym: portfolio.get_historical_data_for_sym(sym) for sym in portfolio.symbol_list}
    historical_fair_px = {sym: portfolio.get_historical_fair_for_sym(sym) for sym in portfolio.symbol_list}
    plot = PlotIndividual(signals,
                          historical_market_price=historical_mkt_prices,
                          historical_fair_price=historical_fair_px)
    plot.plot()


def get_strategy(args) -> Strategy:
    return EquityPrediction(
        MODEL_DIR / "equity_prediction_perc_min.lgb.txt",
        MODEL_DIR / "equity_prediction_perc_max.lgb.txt",
        25,  # lookback
        lookahead=6,   # in days
        frequency=args.frequency,
        min_move_perc=0.05,
        description="EquityRangeML"
    )


if __name__ == "__main__":
    args = parse_args()
    creds = load_credentials(args.credentials)
    if args.name != "":
        logging.basicConfig(filename=DATA_DIR / f"logging/{args.name}.log", level=logging.INFO, force=True)
    strategy = get_strategy(args)
    run(args, creds, strategy)
