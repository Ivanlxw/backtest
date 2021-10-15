"""
Actual file to run for backtesting
"""
from Data.DataWriters.Prices import ABSOLUTE_BT_DATA_DIR
import os, json, queue, logging
from pathlib import Path
from trading.strategy.multiple import MultipleSendAllStrategy

from backtest.broker import AlpacaBroker
from trading.portfolio.portfolio import PercentagePortFolio
from trading.portfolio.rebalance import RebalanceLogicalAny, RebalanceYearly, SellLosersHalfYearly
from trading.portfolio.strategy import LongOnly 
from backtest.utilities.backtest import backtest
from backtest.utilities.utils import MODELINFO_DIR, load_credentials, log_message, parse_args, remove_bs
from trading.data.dataHandler import TDAData
from trading.utilities.enum import OrderType
from backtest.strategy import profitable

ABSOLUTE_LOOP_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
args = parse_args()
load_credentials(args.credentials)
if args.name != "":
    logging.basicConfig(filename=ABSOLUTE_LOOP_DIR /
                        f"Data/logging/{args.name}.log", level=logging.INFO, force=True)

with open(ABSOLUTE_BT_DATA_DIR / "us_stocks.txt") as fin:
    SYM_LIST = list(map(remove_bs, fin.readlines()))
with open(ABSOLUTE_BT_DATA_DIR / "snp500.txt") as fin:
    SYM_LIST += list(map(remove_bs, fin.readlines()))
symbol_list = list(set(SYM_LIST))

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
# Declare the components with respective parameters
NY = "America/New_York"
SG = "Singapore"

bars = TDAData(event_queue, symbol_list, "2015-01-06", live=True)

strategy = MultipleSendAllStrategy(bars, event_queue, [
    profitable.comprehensive_longshort(bars, event_queue),
    profitable.momentum_with_TACross(bars, event_queue),
    profitable.another_TA(bars, event_queue)
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
    order_type=OrderType.MARKET,
    portfolio_strategy=LongOnly,
    rebalance=rebalance_strat
)

if args.live:
    broker = AlpacaBroker(event_queue)
    backtest(
        bars, event_queue, order_queue,
        strategy, port, broker, loop_live=True, sleep_duration=args.sleep_time)
    log_message("saving curr_holdings")
    port.write_curr_holdings()