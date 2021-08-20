"""
Actual file to run for backtesting 
"""
import os
from pathlib import Path
import queue
import random
import logging
from trading.strategy.basic import OneSidedOrderOnly
from trading.utilities.enum import OrderPosition
import pandas as pd
import concurrent.futures as fut

from backtest.broker import SimulatedBroker
from backtest.utilities.utils import generate_start_date, load_credentials, parse_args, remove_bs
from trading.data.dataHandler import HistoricCSVDataHandler
from trading.strategy.statmodels import features, targets, models
from trading.portfolio.portfolio import PercentagePortFolio
from trading.portfolio.rebalance import RebalanceBiennial, RebalanceHalfYearly, RebalanceYearly, SellLongLosersYearly
from trading.portfolio.strategy import LongOnly, SellLowestPerforming
from trading.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy
from trading.strategy import statistics, ta, broad
from backtest.utilities.backtest import backtest

# sklearn modules
from sklearn.linear_model import Ridge, LinearRegression

args = parse_args()
load_credentials(args.credentials)
if args.name != "":
    logging.basicConfig(filename=args.name+'.log', level=logging.INFO)

with open("Data/us_stocks.txt", 'r') as fin:
    stock_list = list(map(remove_bs, fin.readlines()))


def main():
    event_queue = queue.LifoQueue()
    order_queue = queue.Queue()
    start_date = generate_start_date()
    while pd.Timestamp(start_date).dayofweek > 4:
        start_date = generate_start_date()
    print(start_date)
    symbol_list = random.sample(stock_list, 100)

    bars = HistoricCSVDataHandler(event_queue,
                                  symbol_list=symbol_list,
                                  start_date=start_date,
                                  end_date="2020-01-30"
                                  )
    feat = [
        features.RSI(14),
        # features.CCI(20),
        features.RelativePercentile(50),
        features.QuarterlyFundamental(bars, 'roe')
    ]
    target = targets.EMAClosePctChange(30)

    strategy = models.SkLearnRegModelNormalized(
        bars, event_queue, Ridge, feat, target, RebalanceHalfYearly, 
        order_val= 0.05,
        n_history=60,
        params = {
            "fit_intercept": False,
            "alpha": 0.5,
        }
    )
    strategy = MultipleAllStrategy(bars, event_queue, [
        strategy,
        MultipleAnyStrategy(bars, event_queue, [
            ta.TAMax(bars, event_queue, ta.rsi, 14, 5, OrderPosition.BUY),
            ta.TAMin(bars, event_queue, ta.rsi, 14, 6, OrderPosition.SELL),
        ])
    ])
    port = PercentagePortFolio(bars, event_queue, order_queue,
                               percentage=0.10,
                               portfolio_name=(
                                   args.name if args.name != "" else "loop"),
                               mode='asset',
                               expires=7,
                               rebalance=RebalanceYearly,
                               portfolio_strategy=LongOnly,
                               )
    broker = SimulatedBroker(bars, port, event_queue, order_queue)
    backtest(bars, event_queue, order_queue,
             strategy, port, broker, start_date=start_date, show_plot=args.num_runs == 1)


if __name__ == "__main__":
    args = parse_args()
    load_credentials(args.credentials)

    if args.name != "":
        logging.basicConfig(filename=Path(os.environ["WORKSPACE_ROOT"]) /
                            f"Data/logging/{args.name}.log", level=logging.INFO, force=True)
    processes = []
    with fut.ProcessPoolExecutor(4) as e:
        for i in range(args.num_runs):
            processes.append(e.submit(main))
    processes = [p.result() for p in processes]
