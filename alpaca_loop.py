"""
Actual file to run for backtesting
"""
import os, json, queue, logging
from pathlib import Path

from backtest.strategy import profitable
from backtest.utilities.backtest import backtest
from backtest.utilities.utils import MODELINFO_DIR, load_credentials, log_message, parse_args, remove_bs
from Data.DataWriters.Prices import ABSOLUTE_BT_DATA_DIR
from trading.broker.gatekeepers import NoShort, EnoughCash
from trading.strategy.multiple import MultipleSendAllStrategy
from trading.broker.broker import AlpacaBroker
from trading.portfolio.portfolio import PercentagePortFolio
from trading.portfolio.rebalance import RebalanceLogicalAny, RebalanceYearly, SellLosersHalfYearly
from trading.data.dataHandler import DataFromDisk
from trading.utilities.enum import OrderType

args = parse_args()
load_credentials(args.credentials)
if args.name != "":
    logging.basicConfig(filename=Path(os.environ['WORKSPACE_ROOT']) /
                        f"Data/logging/{args.name}.log", level=logging.INFO, force=True)

SYM_LIST = []
sym_filenames = ["snp500.txt", "nasdaq.txt"]
for file in sym_filenames:
    with open(ABSOLUTE_BT_DATA_DIR / file) as fin:
        SYM_LIST += list(map(remove_bs, fin.readlines()))
SYM_LIST = list(set(SYM_LIST))

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
# Declare the components with respective parameters
NY = "America/New_York"
SG = "Singapore"

bars = DataFromDisk(event_queue, SYM_LIST, "2017-01-05", live=True)

strategy = MultipleSendAllStrategy(bars, event_queue, [
    profitable.comprehensive_longshort(bars, event_queue),
    profitable.momentum_with_TACross(bars, event_queue),
])

if args.name != "":
    with open(MODELINFO_DIR / f'{args.name}.json', 'w') as fout:
        fout.write(json.dumps(strategy.describe()))
rebalance_strat = RebalanceLogicalAny(bars, event_queue, [
    SellLosersHalfYearly(bars, event_queue),
    RebalanceYearly(bars, event_queue)
])
port = PercentagePortFolio(
    bars,
    event_queue,
    order_queue,
    percentage=0.05,
    mode="asset",
    expires=3,
    portfolio_name=(args.name if args.name != "" else "alpaca_loop"),
    order_type=OrderType.LIMIT,
    rebalance=rebalance_strat
)

if args.live:
    broker = AlpacaBroker(port, event_queue, gatekeepers=[
        EnoughCash(bars), NoShort(bars)
    ])
    backtest(
        bars, event_queue, order_queue,
        strategy, port, broker, loop_live=True, sleep_duration=args.sleep_time)
    log_message("saving curr_holdings")
    port.write_all_holdings()