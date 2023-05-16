"""
Actual file to run for backtesting
"""
import os
import json
import queue
import logging
from pathlib import Path

# from backtest.strategy import profitable
from backtest.utilities.backtest import Backtest
from backtest.utilities.utils import MODELINFO_DIR, generate_start_date_in_ms, load_credentials, log_message, parse_args, read_universe_list
from trading.broker.gatekeepers import MaxPortfolioPercPerInst, NoShort, EnoughCash
from trading.strategy.fairprice.strategy import FairPriceStrategy
from trading.strategy.fairprice.feature import RelativeCCI, RelativeRSI, TradeImpulseBase, TrendAwareFeatureEMA
from trading.strategy.fairprice.margin import AsymmetricPercentageMargin
from trading.broker.broker import AlpacaBroker
from trading.portfolio.portfolio import PercentagePortFolio
from trading.portfolio.rebalance import RebalanceLogicalAny, RebalanceYearly, SellLosersMonthly
from trading.data.dataHandler import DataFromDisk
from trading.utilities.enum import OrderType

args = parse_args()
creds = load_credentials(args.credentials)
if args.name != "":
    logging.basicConfig(filename=Path(os.environ['DATA_DIR']) /
                        f"logging/{args.name}.log", level=logging.INFO, force=True)

event_queue = queue.LifoQueue()
order_queue = queue.Queue()
# Declare the components with respective parameters
NY = "America/New_York"
SG = "Singapore"

bars = DataFromDisk(read_universe_list(args.universe), creds,
                    generate_start_date_in_ms(2021, 2021))

period = 15     # period to calculate algo
ta_period = 14  # period of calculated values seen 
feature = TrendAwareFeatureEMA(period + ta_period // 2) + RelativeRSI(ta_period, 10) + RelativeCCI(ta_period, 12) #  + TradeImpulseBase(period // 2)
margin = AsymmetricPercentageMargin((0.03, 0.03) if args.frequency == "day" else (0.016, 0.01))
strategy = FairPriceStrategy(bars, feature, margin, period + ta_period)
rebalance_strat = RebalanceLogicalAny(bars, [
    # SellWinnersQuarterly(bars),
    SellLosersMonthly(bars, 0.1), RebalanceYearly(bars)
])
port = PercentagePortFolio(
    0.08,
    rebalance=rebalance_strat,
    mode="asset",
    expires=2,
    portfolio_name=(args.name if args.name != "" else "alpaca_loop"),
    order_type=OrderType.LIMIT,
)

if args.name != "":
    with open(MODELINFO_DIR / f'{args.name}.json', 'w') as fout:
        fout.write(json.dumps(strategy.describe()))


if args.live:
    broker = AlpacaBroker(port, creds, gatekeepers=[
        EnoughCash(), NoShort(), MaxPortfolioPercPerInst(bars, 0.25)
    ])
    bt = Backtest(bars, strategy, port, broker, args)
    bt.run(live=True)
    log_message("saving curr_holdings")
    port.write_curr_holdings()
    port.write_all_holdings()
