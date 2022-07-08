"""
Actual file to run for backtesting
"""
import os
import json
import queue
import logging
from pathlib import Path

from backtest.strategy import profitable
from backtest.utilities.backtest import backtest
from backtest.utilities.utils import MODELINFO_DIR, load_credentials, log_message, parse_args
from trading.broker.gatekeepers import MaxPortfolioPercPerInst, NoShort, EnoughCash
from trading.strategy.multiple import MultipleAnyStrategy, MultipleAllStrategy
from trading.broker.broker import AlpacaBroker
from trading.portfolio.portfolio import PercentagePortFolio
from trading.portfolio.rebalance import RebalanceLogicalAny, RebalanceYearly, SellLosersMonthly
from trading.data.dataHandler import DataFromDisk
from trading.strategy import ta, broad, fundamental, statistics
from trading.utilities.enum import OrderPosition, OrderType
from trading.utilities.utils import get_trading_universe

args = parse_args()
creds = load_credentials(args.credentials)
if args.name != "":
    logging.basicConfig(filename=Path(os.environ['WORKSPACE_ROOT']) /
                        f"Data/logging/{args.name}.log", level=logging.INFO, force=True)

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
# Declare the components with respective parameters
NY = "America/New_York"
SG = "Singapore"

bars = DataFromDisk(event_queue, get_trading_universe(args.universe), creds, "2021-01-05", live=True)

if any("etf" in univ.name for univ in args.universe):
    strategy = profitable.strict_comprehensive_longshort(bars, event_queue, ma_value=22, trending_score=-0.05)
else:
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
    strategy = MultipleAnyStrategy(bars, event_queue, [
            strat_pre_momentum,
            profitable.comprehensive_with_value_bounce(bars, event_queue)
        ])

if args.name != "":
    with open(MODELINFO_DIR / f'{args.name}.json', 'w') as fout:
        fout.write(json.dumps(strategy.describe()))
rebalance_strat = RebalanceLogicalAny(bars, event_queue, [
    # SellWinnersQuarterly(bars, event_queue),
    SellLosersMonthly(bars, event_queue, 0.1),
    RebalanceYearly(bars, event_queue)
])
port = PercentagePortFolio(
    bars,
    event_queue,
    order_queue,
    percentage=0.08,
    mode="asset",
    expires=3,
    portfolio_name=(args.name if args.name != "" else "alpaca_loop"),
    order_type=OrderType.LIMIT,
    rebalance=rebalance_strat
)

if args.live:
    broker = AlpacaBroker(port, event_queue, creds, gatekeepers=[
        EnoughCash(), NoShort(), MaxPortfolioPercPerInst(bars, 0.25)
    ])
    backtest(
        bars, creds, event_queue, order_queue,
        strategy, port, broker, loop_live=True, sleep_duration=args.sleep_time)
    log_message("saving curr_holdings")
    port.write_curr_holdings()
    port.write_all_holdings()
