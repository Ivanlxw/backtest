"""
Actual file to run for backtesting
"""
import os, signal, json, queue, logging
from pathlib import Path

from backtest.broker import AlpacaBroker
from trading.portfolio.portfolio import PercentagePortFolio
from trading.portfolio.rebalance import RebalanceYearly, SellLongLosers, SellLongLosersYearly
from trading.portfolio.strategy import LongOnly, SellLowestPerforming
from backtest.utilities.backtest import backtest
from backtest.utilities.utils import MODELINFO_DIR, load_credentials, log_message, parse_args, remove_bs
from trading.data.dataHandler import TDAData
from trading.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy
from trading.utilities.enum import OrderPosition, OrderType
from trading.strategy import ta, broad, fundamental
from trading.strategy.statistics import ExtremaBounce
from backtest.strategy import profitable

ABSOLUTE_LOOP_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
args = parse_args()
load_credentials(args.credentials)
if args.name != "":
    logging.basicConfig(filename=ABSOLUTE_LOOP_DIR /
                        f"Data/logging/{args.name}.log", level=logging.INFO, force=True)

with open(f"{os.path.abspath(os.path.dirname(__file__))}/Data/us_stocks.txt", 'r') as fin:
    symbol_list = list(map(remove_bs, fin.readlines()))

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
# Declare the components with respective parameters
NY = "America/New_York"
SG = "Singapore"

bars = TDAData(event_queue, symbol_list, "2015-01-06", live=True)

strat_momentum = MultipleAllStrategy(bars, event_queue, [
    ExtremaBounce(bars, event_queue, short_period=6,long_period=80, percentile=50),
    broad.above_sma(bars, event_queue, 'SPY', 25, OrderPosition.BUY),
    # ta.VolAboveSMA(bars, event_queue, 10, OrderPosition.BUY),
    ta.TAMax(bars, event_queue, ta.rsi, 14, 7, OrderPosition.BUY),
    MultipleAnyStrategy(bars, event_queue, [
        fundamental.FundAtLeast(bars, event_queue,
                            'revenueGrowth', 0.1, order_position=OrderPosition.BUY),
        fundamental.FundAtLeast(bars, event_queue, 'roe', 0, order_position=OrderPosition.BUY)
    ])
])

strat_value = MultipleAllStrategy(bars, event_queue, [
    ExtremaBounce(bars, event_queue, short_period=5,long_period=80, percentile=10),
    # RelativeExtrema(bars, event_queue, long_time=50, percentile=10, strat_contrarian=True),
    ta.VolAboveSMA(bars, event_queue, 10, OrderPosition.BUY),
    ta.TAMax(bars, event_queue, ta.rsi, 14, 7, OrderPosition.BUY),  
    MultipleAnyStrategy(bars, event_queue, [
        fundamental.FundAtLeast(bars, event_queue, 'revenueGrowth', 0.03, order_position=OrderPosition.BUY),
        fundamental.FundAtLeast(bars, event_queue, 'netIncomeGrowth', 0.05, order_position=OrderPosition.BUY),    
        fundamental.FundAtLeast(bars, event_queue, 'roe', 0, order_position=OrderPosition.BUY)
    ]),
])

strategy = profitable.comprehensive_longshort(bars, event_queue)

if args.name != "":
    with open(MODELINFO_DIR / f'{args.name}.json', 'w') as fout:
        fout.write(json.dumps(strategy.describe()))

port = PercentagePortFolio(
    bars,
    event_queue,
    order_queue,
    percentage=0.05,
    mode="asset",
    expires=3,
    portfolio_name=(args.name if args.name != "" else "alpaca_loop"),
    order_type=OrderType.MARKET,
    portfolio_strategy=LongOnly,
    rebalance=RebalanceYearly
)

if args.live:
    broker = AlpacaBroker(event_queue)
    backtest(
        bars, event_queue, order_queue,
        strategy, port, broker, loop_live=True)
    log_message("saving curr_holdings")
    port.write_curr_holdings()