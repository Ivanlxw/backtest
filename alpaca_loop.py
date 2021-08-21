"""
Actual file to run for backtesting
"""
from Data.DataWriter import ABSOLUTE_BT_DATA_DIR
import os, json, queue, logging
from pathlib import Path
from trading.strategy.multiple import MultipleSendAllStrategy

from backtest.broker import AlpacaBroker
from trading.portfolio.portfolio import PercentagePortFolio
from trading.portfolio.rebalance import RebalanceYearly
from trading.portfolio.strategy import LongOnly 
from backtest.utilities.backtest import backtest
from backtest.utilities.utils import MODELINFO_DIR, load_credentials, log_message, parse_args, remove_bs
from trading.data.dataHandler import TDAData
from trading.utilities.enum import OrderPosition, OrderType

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

strategy = MultipleSendAllStrategy(bars, event_queue, [
    # insert strategy here
])

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
    port.write_all_holdings(ABSOLUTE_BT_DATA_DIR/ f"portfolio/{port.name}.json")