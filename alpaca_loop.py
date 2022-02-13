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
from trading.broker.gatekeepers import MaxPortfolioValuePerInst, NoShort, EnoughCash
from trading.strategy.multiple import MultipleSendAllStrategy
from trading.broker.broker import AlpacaBroker
from trading.portfolio.portfolio import PercentagePortFolio
from trading.portfolio.rebalance import RebalanceLogicalAny, RebalanceYearly, SellLosersHalfYearly, SellWinnersQuarterly
from trading.data.dataHandler import DataFromDisk
from trading.utilities.enum import OrderType
from trading.utilities.utils import DOW_LIST, SNP_LIST, NASDAQ_LIST, ETF_LIST

args = parse_args()
load_credentials(args.credentials)
if args.name != "":
    logging.basicConfig(filename=Path(os.environ['WORKSPACE_ROOT']) /
                        f"Data/logging/{args.name}.log", level=logging.INFO, force=True)

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
# Declare the components with respective parameters
NY = "America/New_York"
SG = "Singapore"

bars = DataFromDisk(event_queue, DOW_LIST + SNP_LIST +
                    NASDAQ_LIST + ETF_LIST, "2021-01-05", live=True)

strategy = MultipleSendAllStrategy(bars, event_queue, [
    # profitable.comprehensive_longshort(bars, event_queue),
    profitable.high_beta_momentum(bars, event_queue),
    profitable.dcf_value_growth(bars, event_queue),
    profitable.momentum_with_TACross(bars, event_queue),
    profitable.strict_comprehensive_momentum(bars, event_queue)
])

if args.name != "":
    with open(MODELINFO_DIR / f'{args.name}.json', 'w') as fout:
        fout.write(json.dumps(strategy.describe()))
rebalance_strat = RebalanceLogicalAny(bars, event_queue, [
    SellWinnersQuarterly(bars, event_queue),
    SellLosersHalfYearly(bars, event_queue, 0.15),
    RebalanceYearly(bars, event_queue)
])
port = PercentagePortFolio(
    bars,
    event_queue,
    order_queue,
    percentage=0.07,
    mode="asset",
    expires=3,
    portfolio_name=(args.name if args.name != "" else "alpaca_loop"),
    order_type=OrderType.LIMIT,
    rebalance=rebalance_strat
)

if args.live:
    broker = AlpacaBroker(port, event_queue, gatekeepers=[
        EnoughCash(bars), NoShort(bars), MaxPortfolioValuePerInst(bars, 0.10)
    ])
    backtest(
        bars, event_queue, order_queue,
        strategy, port, broker, loop_live=True, sleep_duration=args.sleep_time)
    log_message("saving curr_holdings")
    port.write_curr_holdings()
    port.write_all_holdings()
